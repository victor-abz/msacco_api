# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import datetime
import json
import random
import re
import string
import traceback
from contextlib import contextmanager
from time import time

import frappe
import frappe.defaults
import frappe.model.meta
from frappe import _
from frappe.exceptions import DoesNotExistError, ImplicitCommitError
from frappe.model.utils.link_count import flush_local_link_count
from frappe.utils import cast as cast_fieldtype
from frappe.utils import get_datetime, get_table_name, getdate, now, sbool
from pypika.dialects import MySQLQueryBuilder, PostgreSQLQueryBuilder
from pypika.terms import Criterion, NullValue

import msacco_api
from msacco_api.database.utils import (
    EmptyQueryValues,
    FallBackDateTimeStr,
    LazyMogrify,
    Query,
    QueryValues,
    is_query_type,
)
from msacco_api.query_builder import (
    get_qb_engine,
    get_query_builder,
    patch_query_aggregation,
    patch_query_execute,
)
from msacco_api.query_builder.functions import Count

IFNULL_PATTERN = re.compile(r"ifnull\(", flags=re.IGNORECASE)
INDEX_PATTERN = re.compile(r"\s*\([^)]+\)\s*")
SINGLE_WORD_PATTERN = re.compile(r'([`"]?)(tab([A-Z]\w+))\1')
MULTI_WORD_PATTERN = re.compile(r'([`"])(tab([A-Z]\w+)( [A-Z]\w+)+)\1')
frappe._cbs_qb_patched = {}
# frappe.cbs_qb.engine = get_qb_engine()

frappe.cbs_qb = get_query_builder("postgres")
frappe.cbs_qb.engine = get_qb_engine()

if not frappe._cbs_qb_patched.get("postgres"):
    patch_query_execute()
    patch_query_aggregation()


class CBSDatabase:
    """
    Open a database connection with the given parmeters, if use_default is True, use the
    login details from `conf.py`. This is called by the request handler and is accessible using
    the `db` global variable. the `sql` method is also global to run queries
    """

    VARCHAR_LEN = 140
    MAX_COLUMN_LENGTH = 64

    OPTIONAL_COLUMNS = ["_user_tags", "_comments", "_assign", "_liked_by"]
    DEFAULT_SHORTCUTS = [
        "_Login",
        "__user",
        "_Full Name",
        "Today",
        "__today",
        "now",
        "Now",
    ]
    STANDARD_VARCHAR_COLUMNS = ("name", "owner", "modified_by")
    DEFAULT_COLUMNS = [
        "name",
        "creation",
        "modified",
        "modified_by",
        "owner",
        "docstatus",
        "idx",
    ]
    CHILD_TABLE_COLUMNS = ("parent", "parenttype", "parentfield")
    MAX_WRITES_PER_TRANSACTION = 200_000

    # NOTE:
    # FOR MARIADB - using no cache - as during backup, if the sequence was used in anyform,
    # it drops the cache and uses the next non cached value in setval query and
    # puts that in the backup file, which will start the counter
    # from that value when inserting any new record in the doctype.
    # By default the cache is 1000 which will mess up the sequence when
    # using the system after a restore.
    #
    # Another case could be if the cached values expire then also there is a chance of
    # the cache being skipped.
    #
    # FOR POSTGRES - The sequence cache for postgres is per connection.
    # Since we're opening and closing connections for every request this results in skipping the cache
    # to the next non-cached value hence not using cache in postgres.
    # ref: https://stackoverflow.com/questions/21356375/postgres-9-0-4-sequence-skipping-numbers
    SEQUENCE_CACHE = 0

    class InvalidColumnName(frappe.ValidationError):
        pass

    def __init__(
        self,
        host=None,
        user=None,
        password=None,
        ac_name=None,
        use_default=0,
        port=None,
    ):
        self.setup_type_map()
        self.host = host or frappe.conf.corebanking_db_host or "127.0.0.1"
        self.port = port or frappe.conf.corebanking_db_port or ""
        self.user = user
        self.db_name = frappe.conf.corebanking_db_name
        self._conn = None

        if ac_name:
            self.user = ac_name or frappe.conf.corebanking_db_name

        if use_default:
            self.user = frappe.conf.corebanking_db_name

        self.transaction_writes = 0
        self.auto_commit_on_many_writes = 0

        self.password = password
        self.value_cache = {}
        # self.db_type: str
        # self.last_query (lazy) attribute of last sql query executed

    def setup_type_map(self):
        pass

    def connect(self):
        """Connects to a database as set in `site_config.json`."""
        self.cur_db_name = self.user
        self._conn = self.get_connection()
        self._cursor = self._conn.cursor()
        frappe.local.rollback_observers = []

    def use(self, db_name):
        """`USE` db_name."""
        self._conn.select_db(db_name)

    def get_connection(self):
        """Returns a Database connection object that conforms with https://peps.python.org/pep-0249/#connection-objects"""
        raise NotImplementedError

    def get_database_size(self):
        raise NotImplementedError

    def _transform_query(self, query: Query, values: QueryValues) -> tuple:
        return query, values

    def _transform_result(self, result: list[tuple]) -> list[tuple]:
        return result

    def sql(
        self,
        query: Query,
        values: QueryValues = EmptyQueryValues,
        as_dict=0,
        as_list=0,
        formatted=0,
        debug=0,
        ignore_ddl=0,
        as_utf8=0,
        auto_commit=0,
        update=None,
        explain=False,
        run=True,
        pluck=False,
    ):
        """Execute a SQL query and fetch all rows.

        :param query: SQL query.
        :param values: Tuple / List / Dict of values to be escaped and substituted in the query.
        :param as_dict: Return as a dictionary.
        :param as_list: Always return as a list.
        :param formatted: Format values like date etc.
        :param debug: Print query and `EXPLAIN` in debug log.
        :param ignore_ddl: Catch exception if table, column missing.
        :param as_utf8: Encode values as UTF 8.
        :param auto_commit: Commit after executing the query.
        :param update: Update this dict to all rows (if returned `as_dict`).
        :param run: Returns query without executing it if False.
        Examples:

                # return customer names as dicts
                frappe.cbs_db.sql("select name from tabCustomer", as_dict=True)

                # return names beginning with a
                frappe.cbs_db.sql("select name from tabCustomer where name like %s", "a%")

                # values as dict
                frappe.cbs_db.sql("select name from tabCustomer where name like %(name)s and owner=%(owner)s",
                        {"name": "a%", "owner":"test@example.com"})

        """
        if isinstance(query, (MySQLQueryBuilder, PostgreSQLQueryBuilder)):
            frappe.errprint(
                "Use run method to execute SQL queries generated by Query Engine"
            )

        debug = debug or getattr(self, "debug", False)
        query = str(query)
        if not run:
            return query

        # remove whitespace / indentation from start and end of query
        query = query.strip()

        # replaces ifnull in query with coalesce
        query = IFNULL_PATTERN.sub("coalesce(", query)

        if not self._conn:
            self.connect()

        # in transaction validations
        self.check_transaction_status(query)
        self.clear_db_table_cache(query)

        if auto_commit:
            self.commit()

        if debug:
            time_start = time()

        if values == EmptyQueryValues:
            values = None
        elif not isinstance(values, (tuple, dict, list)):
            values = (values,)
        query, values = self._transform_query(query, values)

        try:
            self._cursor.execute(query, values)
        except Exception as e:
            if self.is_syntax_error(e):
                frappe.errprint(f"Syntax error in query:\n{query} {values}")

            elif self.is_deadlocked(e):
                raise frappe.QueryDeadlockError(e) from e

            elif self.is_timedout(e):
                raise frappe.QueryTimeoutError(e) from e

            elif self.is_read_only_mode_error(e):
                frappe.throw(
                    _(
                        "Site is running in read only mode, this action can not be performed right now. Please try again later."
                    ),
                    title=_("In Read Only Mode"),
                    exc=frappe.InReadOnlyMode,
                )

            # TODO: added temporarily
            elif self.db_type == "postgres":
                traceback.print_stack()
                frappe.errprint(f"Error in query:\n{e}")
                raise

            elif isinstance(e, self.ProgrammingError):
                if frappe.conf.developer_mode:
                    traceback.print_stack()
                    frappe.errprint(f"Error in query:\n{query, values}")
                raise

            if not (
                ignore_ddl
                and (
                    self.is_missing_column(e)
                    or self.is_table_missing(e)
                    or self.cant_drop_field_or_key(e)
                )
            ):
                raise

        if debug:
            time_end = time()
            frappe.errprint(f"Execution time: {time_end - time_start:.2f} sec")

        self.log_query(query, values, debug, explain)

        if auto_commit:
            self.commit()

        if not self._cursor.description:
            return ()

        self.last_result = self._transform_result(self._cursor.fetchall())

        if pluck:
            return [r[0] for r in self.last_result]

        # scrub output if required
        if as_dict:
            ret = self.fetch_as_dict(formatted, as_utf8)
            if update:
                for r in ret:
                    r.update(update)
            return ret
        elif as_list or as_utf8:
            return self.convert_to_lists(self.last_result, formatted, as_utf8)
        return self.last_result

    def _log_query(
        self, mogrified_query: str, debug: bool = False, explain: bool = False
    ) -> None:
        """Takes the query and logs it to various interfaces according to the settings."""
        _query = None

        if frappe.conf.allow_tests and frappe.cache().get_value("flag_print_sql"):
            _query = _query or str(mogrified_query)

        if debug:
            _query = _query or str(mogrified_query)
            if explain and is_query_type(_query, "select"):
                self.explain_query(_query)
            frappe.errprint(_query)

        if frappe.conf.logging == 2:
            _query = _query or str(mogrified_query)
            frappe.log(f"<<<< query\n{_query}\n>>>>")

        if frappe.flags.in_migrate:
            _query = _query or str(mogrified_query)
            self.log_touched_tables(_query)

    def log_query(
        self,
        query: str,
        values: QueryValues = None,
        debug: bool = False,
        explain: bool = False,
    ) -> str:
        # TODO: Use mogrify until MariaDB Connector/C 1.1 is released and we can fetch something
        # like cursor._transformed_statement from the cursor object. We can also avoid setting
        # mogrified_query if we don't need to log it.
        mogrified_query = self.lazy_mogrify(query, values)
        self._log_query(mogrified_query, debug, explain)
        return mogrified_query

    def mogrify(self, query: Query, values: QueryValues):
        """build the query string with values"""
        if not values:
            return query

        try:
            return self._cursor.mogrify(query, values)
        except AttributeError:
            if isinstance(values, dict):
                return query % {
                    k: frappe.cbs_db.escape(v) if isinstance(v, str) else v
                    for k, v in values.items()
                }
            elif isinstance(values, (list, tuple)):
                return query % tuple(
                    frappe.cbs_db.escape(v) if isinstance(v, str) else v for v in values
                )
            return query, values

    def lazy_mogrify(self, query: Query, values: QueryValues) -> LazyMogrify:
        """Wrap the object with str to generate mogrified query."""
        return LazyMogrify(query, values)

    def explain_query(self, query, values=None):
        """Print `EXPLAIN` in error log."""
        frappe.errprint("--- query explain ---")
        try:
            self._cursor.execute(f"EXPLAIN {query}", values)
        except Exception as e:
            frappe.errprint(f"error in query explain: {e}")
        else:
            frappe.errprint(json.dumps(self.fetch_as_dict(), indent=1))
            frappe.errprint("--- query explain end ---")

    def sql_list(self, query, values=(), debug=False, **kwargs):
        """Return data as list of single elements (first column).

        Example:

                # doctypes = ["DocType", "DocField", "User", ...]
                doctypes = frappe.cbs_db.sql_list("select name from DocType")
        """
        return self.sql(query, values, **kwargs, debug=debug, pluck=True)

    def sql_ddl(self, query, debug=False):
        """Commit and execute a query. DDL (Data Definition Language) queries that alter schema
        autocommit in MariaDB."""
        self.commit()
        self.sql(query, debug=debug)

    def check_transaction_status(self, query):
        """Raises exception if more than 20,000 `INSERT`, `UPDATE` queries are
        executed in one transaction. This is to ensure that writes are always flushed otherwise this
        could cause the system to hang."""
        self.check_implicit_commit(query)

        if query and is_query_type(query, ("commit", "rollback")):
            self.transaction_writes = 0

        if query[:6].lower() in ("update", "insert", "delete"):
            self.transaction_writes += 1
            if self.transaction_writes > self.MAX_WRITES_PER_TRANSACTION:
                if self.auto_commit_on_many_writes:
                    self.commit()
                else:
                    msg = (
                        "<br><br>"
                        + _("Too many changes to database in single action.")
                        + "<br>"
                    )
                    msg += _("The changes have been reverted.") + "<br>"
                    raise frappe.TooManyWritesError(msg)

    def check_implicit_commit(self, query):
        if (
            self.transaction_writes
            and query
            and is_query_type(
                query, ("start", "alter", "drop", "create", "begin", "truncate")
            )
        ):
            raise ImplicitCommitError("This statement can cause implicit commit")

    def fetch_as_dict(self, formatted=0, as_utf8=0) -> list[frappe._dict]:
        """Internal. Converts results to dict."""
        result = self.last_result
        ret = []
        if result:
            keys = [column[0] for column in self._cursor.description]

        for r in result:
            values = []
            for value in r:
                if as_utf8 and isinstance(value, str):
                    value = value.encode("utf-8")
                values.append(value)

            ret.append(frappe._dict(zip(keys, values)))
        return ret

    @staticmethod
    def clear_db_table_cache(query):
        if query and is_query_type(query, ("drop", "create")):
            frappe.cache().delete_key("db_tables")

    @staticmethod
    def needs_formatting(result, formatted):
        """Returns true if the first row in the result has a Date, Datetime, Long Int."""
        if result and result[0]:
            for v in result[0]:
                if isinstance(
                    v, (datetime.date, datetime.timedelta, datetime.datetime, int)
                ):
                    return True
                if formatted and isinstance(v, (int, float)):
                    return True

        return False

    def get_description(self):
        """Returns result metadata."""
        return self._cursor.description

    @staticmethod
    def convert_to_lists(res, formatted=0, as_utf8=0):
        """Convert tuple output to lists (internal)."""
        nres = []
        for r in res:
            nr = []
            for val in r:
                if as_utf8 and isinstance(val, str):
                    val = val.encode("utf-8")
                nr.append(val)
            nres.append(nr)
        return nres

    def get(self, doctype, filters=None, as_dict=True, cache=False):
        """Returns `get_value` with fieldname='*'"""
        return self.get_value(doctype, filters, "*", as_dict=as_dict, cache=cache)

    def get_value(
        self,
        doctype,
        filters=None,
        fieldname="name",
        ignore=None,
        as_dict=False,
        debug=False,
        order_by="KEEP_DEFAULT_ORDERING",
        cache=False,
        for_update=False,
        *,
        run=True,
        pluck=False,
        distinct=False,
    ):
        """Returns a document property or list of properties.

        :param doctype: DocType name.
        :param filters: Filters like `{"x":"y"}` or name of the document. `None` if Single DocType.
        :param fieldname: Column name.
        :param ignore: Don't raise exception if table, column is missing.
        :param as_dict: Return values as dict.
        :param debug: Print query in error log.
        :param order_by: Column to order by

        Example:

                # return first customer starting with a
                frappe.cbs_db.get_value("Customer", {"name": ("like a%")})

                # return last login of **User** `test@example.com`
                frappe.cbs_db.get_value("User", "test@example.com", "last_login")

                last_login, last_ip = frappe.cbs_db.get_value("User", "test@example.com",
                        ["last_login", "last_ip"])

                # returns default date_format
                frappe.cbs_db.get_value("System Settings", None, "date_format")
        """

        result = self.get_values(
            doctype,
            filters,
            fieldname,
            ignore,
            as_dict,
            debug,
            order_by,
            cache=cache,
            for_update=for_update,
            run=run,
            pluck=pluck,
            distinct=distinct,
            limit=1,
        )

        if not run:
            return result

        if not result:
            return None

        row = result[0]

        if len(row) > 1 or as_dict:
            return row
        # single field is requested, send it without wrapping in containers
        return row[0]

    def get_values(
        self,
        doctype,
        filters=None,
        fieldname="*",
        ignore=None,
        as_dict=False,
        debug=False,
        order_by=None,
        update=None,
        cache=False,
        for_update=False,
        *,
        run=True,
        pluck=False,
        distinct=False,
        limit=None,
    ):
        """Returns multiple document properties.

        :param doctype: DocType name.
        :param filters: Filters like `{"x":"y"}` or name of the document.
        :param fieldname: Column name.
        :param ignore: Don't raise exception if table, column is missing.
        :param as_dict: Return values as dict.
        :param debug: Print query in error log.
        :param order_by: Column to order by,
        :param distinct: Get Distinct results.

        Example:

                # return first customer starting with a
                customers = frappe.cbs_db.get_values("Customer", {"name": ("like a%")})

                # return last login of **User** `test@example.com`
                user = frappe.cbs_db.get_values("User", "test@example.com", "*")[0]
        """
        out = None
        if (
            cache
            and isinstance(filters, str)
            and (doctype, filters, fieldname) in self.value_cache
        ):
            return self.value_cache[(doctype, filters, fieldname)]

        if distinct:
            order_by = None

        if isinstance(filters, list):
            out = self._get_value_for_many_names(
                doctype=doctype,
                names=filters,
                field=fieldname,
                order_by=order_by,
                debug=debug,
                run=run,
                pluck=pluck,
                distinct=distinct,
                limit=limit,
                as_dict=as_dict,
            )

        else:
            fields = fieldname
            if fieldname != "*":
                if isinstance(fieldname, str):
                    fields = [fieldname]

            # if (filters is not None) and (filters != doctype or doctype == "DocType"):
            try:

                if order_by:
                    order_by = (
                        "modified" if order_by == "KEEP_DEFAULT_ORDERING" else order_by
                    )
                out = self._get_values_from_table(
                    fields=fields,
                    filters=filters,
                    doctype=doctype,
                    as_dict=as_dict,
                    debug=debug,
                    order_by=order_by,
                    update=update,
                    for_update=for_update,
                    run=run,
                    pluck=pluck,
                    distinct=distinct,
                    limit=limit,
                )
            except Exception as e:
                if ignore and (
                    frappe.cbs_db.is_missing_column(e)
                    or frappe.cbs_db.is_table_missing(e)
                ):
                    # table or column not found, return None
                    out = None

                else:
                    raise

        if cache and isinstance(filters, str):
            self.value_cache[(doctype, filters, fieldname)] = out

        return out

    def _get_values_from_table(
        self,
        fields,
        filters,
        doctype,
        as_dict,
        *,
        debug=False,
        order_by=None,
        update=None,
        for_update=False,
        run=True,
        pluck=False,
        distinct=False,
        limit=None,
    ):
        field_objects = []
        query = frappe.cbs_qb.engine.get_query(
            table=doctype,
            filters=filters,
            orderby=order_by,
            for_update=for_update,
            field_objects=field_objects,
            fields=fields,
            distinct=distinct,
            limit=limit,
        )
        if (
            fields == "*"
            and not isinstance(fields, (list, tuple))
            and not isinstance(fields, Criterion)
        ):
            as_dict = True
        return query.run(
            as_dict=as_dict, debug=debug, update=update, run=run, pluck=pluck
        )

    def _get_value_for_many_names(
        self,
        doctype,
        names,
        field,
        order_by,
        *,
        debug=False,
        run=True,
        pluck=False,
        distinct=False,
        limit=None,
        as_dict=False,
    ):
        if names := list(filter(None, names)):
            return frappe.cbs_qb.engine.get_query(
                doctype,
                fields=field,
                filters=names,
                order_by=order_by,
                pluck=pluck,
                distinct=distinct,
                limit=limit,
            ).run(debug=debug, run=run, as_dict=as_dict)
        return {}

    def begin(self, *, read_only=False):
        read_only = read_only or frappe.flags.read_only
        mode = "READ ONLY" if read_only else ""
        self.sql(f"START TRANSACTION {mode}")

    def commit(self):
        """Commit current transaction. Calls SQL `COMMIT`."""
        for method in frappe.local.before_commit:
            frappe.call(method[0], *(method[1] or []), **(method[2] or {}))

        self.sql("commit")
        self.begin()  # explicitly start a new transaction

        frappe.local.rollback_observers = []
        self.flush_realtime_log()
        enqueue_jobs_after_commit()
        flush_local_link_count()

    def add_before_commit(self, method, args=None, kwargs=None):
        frappe.local.before_commit.append([method, args, kwargs])

    @staticmethod
    def flush_realtime_log():
        for args in frappe.local.realtime_log:
            frappe.realtime.emit_via_redis(*args)

        frappe.local.realtime_log = []

    def savepoint(self, save_point):
        """Savepoints work as a nested transaction.

        Changes can be undone to a save point by doing frappe.cbs_db.rollback(save_point)

        Note: rollback watchers can not work with save points.
                so only changes to database are undone when rolling back to a savepoint.
                Avoid using savepoints when writing to filesystem."""
        self.sql(f"savepoint {save_point}")

    def release_savepoint(self, save_point):
        self.sql(f"release savepoint {save_point}")

    def rollback(self, *, save_point=None):
        """`ROLLBACK` current transaction. Optionally rollback to a known save_point."""
        if save_point:
            self.sql(f"rollback to savepoint {save_point}")
        else:
            self.sql("rollback")
            self.begin()
            for obj in dict.fromkeys(frappe.local.rollback_observers):
                if hasattr(obj, "on_rollback"):
                    obj.on_rollback()
            frappe.local.rollback_observers = []

    def field_exists(self, dt, fn):
        """Return true of field exists."""
        return self.exists("DocField", {"fieldname": fn, "parent": dt})

    def table_exists(self, doctype, cached=True):
        """Returns True if table for given doctype exists."""
        return f"tab{doctype}" in self.get_tables(cached=cached)

    def has_table(self, doctype):
        return self.table_exists(doctype)

    def get_tables(self, cached=True):
        raise NotImplementedError

    def a_row_exists(self, doctype):
        """Returns True if atleast one row exists."""
        return frappe.get_all(doctype, limit=1, order_by=None, as_list=True)

    def exists(self, dt, dn=None, cache=False):
        """Return the document name of a matching document, or None.

        Note: `cache` only works if `dt` and `dn` are of type `str`.

        ## Examples

        Pass doctype and docname (only in this case we can cache the result)

        ```
        exists("User", "jane@example.org", cache=True)
        ```

        Pass a dict of filters including the `"doctype"` key:

        ```
        exists({"doctype": "User", "full_name": "Jane Doe"})
        ```

        Pass the doctype and a dict of filters:

        ```
        exists("User", {"full_name": "Jane Doe"})
        ```
        """
        if dt != "DocType" and dt == dn:
            # single always exists (!)
            return dn

        if isinstance(dt, dict):
            dt = dt.copy()  # don't modify the original dict
            dt, dn = dt.pop("doctype"), dt

        return self.get_value(dt, dn, ignore=True, cache=cache)

    def count(self, dt, filters=None, debug=False, cache=False, distinct: bool = True):
        """Returns `COUNT(*)` for given DocType and filters."""
        if cache and not filters:
            cache_count = frappe.cache().get_value(f"doctype:count:{dt}")
            if cache_count is not None:
                return cache_count
        count = frappe.cbs_qb.engine.get_query(
            table=dt, filters=filters, fields=Count("*"), distinct=distinct
        ).run(debug=debug)[0][0]
        if not filters and cache:
            frappe.cache().set_value(f"doctype:count:{dt}", count, expires_in_sec=86400)
        return count

    @staticmethod
    def format_date(date):
        return getdate(date).strftime("%Y-%m-%d")

    @staticmethod
    def format_datetime(datetime):
        if not datetime:
            return FallBackDateTimeStr

        if isinstance(datetime, str):
            if ":" not in datetime:
                datetime = datetime + " 00:00:00.000000"
        else:
            datetime = datetime.strftime("%Y-%m-%d %H:%M:%S.%f")

        return datetime

    def get_db_table_columns(self, table) -> list[str]:
        """Returns list of column names from given table."""
        columns = frappe.cache().hget("table_columns", table)
        if columns is None:
            information_schema = frappe.cbs_qb.Schema("information_schema")

            columns = (
                frappe.cbs_qb.from_(information_schema.columns)
                .select(information_schema.columns.column_name)
                .where(information_schema.columns.table_name == table)
                .run(pluck=True)
            )

            if columns:
                frappe.cache().hset("table_columns", table, columns)

        return columns

    def get_table_columns(self, doctype):
        """Returns list of column names from given doctype."""
        columns = self.get_db_table_columns(doctype)
        if not columns:
            raise self.TableMissingError("Table", doctype)
        return columns

    def has_column(self, doctype, column):
        """Returns True if column exists in database."""
        return column in self.get_table_columns(doctype)

    def get_column_type(self, doctype, column):
        """Returns column type from database."""
        information_schema = frappe.cbs_qb.Schema("information_schema")
        table = get_table_name(doctype)

        return (
            frappe.cbs_qb.from_(information_schema.columns)
            .select(information_schema.columns.column_type)
            .where(
                (information_schema.columns.table_name == table)
                & (information_schema.columns.column_name == column)
            )
            .run(pluck=True)[0]
        )

    def has_index(self, table_name, index_name):
        raise NotImplementedError

    def add_index(self, doctype, fields, index_name=None):
        raise NotImplementedError

    def add_unique(self, doctype, fields, constraint_name=None):
        raise NotImplementedError

    @staticmethod
    def get_index_name(fields):
        index_name = "_".join(fields) + "_index"
        # remove index length if present e.g. (10) from index name
        return INDEX_PATTERN.sub(r"", index_name)

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._cursor = None
            self._conn = None

    @staticmethod
    def escape(s, percent=True):
        """Excape quotes and percent in given string."""
        # implemented in specific class
        raise NotImplementedError

    @staticmethod
    def is_column_missing(e):
        return frappe.cbs_db.is_missing_column(e)

    def get_descendants(self, doctype, name):
        """Return descendants of the group node in tree"""
        from frappe.utils.nestedset import get_descendants_of

        try:
            return get_descendants_of(doctype, name, ignore_permissions=True)
        except Exception:
            # Can only happen if document doesn't exists - kept for backward compatibility
            return []

    def is_missing_table_or_column(self, e):
        return self.is_missing_column(e) or self.is_table_missing(e)

    def get_last_created(self, doctype):
        last_record = self.get_all(
            doctype, ("creation"), limit=1, order_by="creation desc"
        )
        if last_record:
            return get_datetime(last_record[0].creation)
        else:
            return None

    def bulk_insert(
        self, doctype, fields, values, ignore_duplicates=False, *, chunk_size=10_000
    ):
        """
        Insert multiple records at a time

        :param doctype: Doctype name
        :param fields: list of fields
        :params values: list of list of values
        """
        values = list(values)
        table = frappe.cbs_qb.DocType(doctype)

        for start_index in range(0, len(values), chunk_size):
            query = frappe.cbs_qb.into(table)
            if ignore_duplicates:
                query = query.on_conflict().do_nothing()

            values_to_insert = values[start_index : start_index + chunk_size]
            query.columns(fields).insert(*values_to_insert).run()


def enqueue_jobs_after_commit():
    from frappe.utils.background_jobs import (
        RQ_JOB_FAILURE_TTL,
        RQ_RESULTS_TTL,
        execute_job,
        get_queue,
    )

    if frappe.flags.enqueue_after_commit and len(frappe.flags.enqueue_after_commit) > 0:
        for job in frappe.flags.enqueue_after_commit:
            q = get_queue(job.get("queue"), is_async=job.get("is_async"))
            q.enqueue_call(
                execute_job,
                timeout=job.get("timeout"),
                kwargs=job.get("queue_args"),
                failure_ttl=RQ_JOB_FAILURE_TTL,
                result_ttl=RQ_RESULTS_TTL,
            )
        frappe.flags.enqueue_after_commit = []


@contextmanager
def savepoint(catch: type | tuple[type, ...] = Exception):
    """Wrapper for wrapping blocks of DB operations in a savepoint.

    as contextmanager:

    for doc in docs:
            with savepoint(catch=DuplicateError):
                    doc.insert()

    as decorator (wraps FULL function call):

    @savepoint(catch=DuplicateError)
    def process_doc(doc):
            doc.insert()
    """
    try:
        savepoint = "".join(random.sample(string.ascii_lowercase, 10))
        frappe.cbs_db.savepoint(savepoint)
        yield  # control back to calling function
    except catch:
        frappe.cbs_db.rollback(save_point=savepoint)
    else:
        frappe.cbs_db.release_savepoint(savepoint)

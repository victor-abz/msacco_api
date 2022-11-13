import frappe
from msacco_api.cbs_db import PostgresDatabase

def get_cbs_root_connection(root_login, root_password):
	import getpass

	if not frappe.local.flags.root_cbs_connection:
		if not root_login:
			root_login = "root"

		if not root_password:
			root_password = frappe.conf.get("root_password") or None

		if not root_password:
			root_password = getpass.getpass("Corebanking root password: ")

		frappe.local.flags.root_cbs_connection = PostgresDatabase(
			user=root_login, password=root_password
		)

	return frappe.local.flags.root_cbs_connection


def check_connection():
	root_conn = get_cbs_root_connection('victor', '123456')
	frappe.cbs_db = root_conn
	print(root_conn.describe('ad_cli'))
	# print(root_conn.sql("SELECT * FROM ad_cli WHERE id_client = '10931'", as_dict=1))
	print(len(root_conn.get_values("ad_cpt", filters={"solde": ["!=", "0"]}, fieldname=["*"], order_by="id_titulaire asc", as_dict=1)))
	# print(len(frappe.cbs_db.get_values("ad_cli")))
	root_conn.close()
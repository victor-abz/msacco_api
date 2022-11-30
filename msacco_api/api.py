import json

import frappe
import requests

from msacco_api import get_wsse
from msacco_api.cbs_db import PostgresDatabase

ID_CLIENT_LENGTH = 8  # Length Without Agence


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
    root_conn = get_cbs_root_connection(
        frappe.conf.get("corebanking_db_username"), frappe.conf.get("corebanking_db_pw")
    )
    frappe.cbs_db = root_conn
    # print(root_conn.describe('ad_cli'))
    # print(root_conn.sql("SELECT * FROM ad_cli WHERE id_client = '10931'", as_dict=1))
    # print(len(root_conn.get_values("ad_cpt", filters={"solde": ["!=", "0"]}, fieldname=["*"], order_by="id_titulaire asc", as_dict=1)))
    # print(len(frappe.cbs_db.get_values("ad_cli")))

    # get all current accounts balance greater than 0

    # Accounts in Mobile Bankings have their balance already transferred
    # print(frappe.cbs_qb)
    accounts_in_mob_banking = frappe.cbs_db.get_values(
        "ad_abonnement",
        fieldname=["id_client"],
        order_by="id_client asc",
        as_dict=1,
        pluck="id_client",
    )
    frappe.cbs_db.commit()
    # print(",.<>")

    balances_to_transfer = frappe.cbs_db.get_values(
        "ad_cpt",
        filters={
            "solde": [">", "0"],
            "etat_cpte": 1,
            "id_prod": 1,
            "id_titulaire": ["not in", accounts_in_mob_banking],
        },
        fieldname=["*"],
        order_by="id_titulaire asc",
        as_dict=1,
        # debug=True,
    )
    frappe.cbs_db.commit()

    # Get Total sum to be transferred
    total_to_transfer = frappe.cbs_db.get_values(
        "ad_cpt",
        filters={
            "solde": [">", "0"],
            "etat_cpte": 1,
            "id_prod": 1,
            "id_titulaire": ["not in", accounts_in_mob_banking],
        },
        fieldname=["sum(solde)"],
        as_dict=1,
        # debug=True,
    )

    # Get Total compulsor sum to be transferred
    comp_balance_before_transfer = frappe.cbs_db.get_values(
        "ad_cpt",
        filters={
            "solde": [">", "0"],
            "etat_cpte": 1,
            "id_prod": 7,
            "id_titulaire": ["not in", accounts_in_mob_banking],
        },
        fieldname=["sum(solde)"],
        as_dict=1,
        # debug=True,
    )

    # Get Full Account No:
    logs = frappe.new_doc("Savings migration Logs")

    logs.total_to_transfer = total_to_transfer[0].sum
    logs.total_accounts_to_transfer = len(balances_to_transfer)

    logs.comp_balance_before = comp_balance_before_transfer[0].sum

    total_account_transfered = 0
    total_transferred = 0
    url = f'http://{frappe.conf.get("api_url")}/api/v1/client/transfert/compte'

    # Do the Transfer
    count = 1
    for account in balances_to_transfer:
        print(f"{count} of {len(balances_to_transfer)}")
        count += 1

        # Format ID Client to 8 digits with Agence ID
        identifiant_client = f'1{f"{account.id_titulaire}".zfill(ID_CLIENT_LENGTH)}'

        # Get num Compulsory
        compulsory = frappe.cbs_db.get_values(
            "ad_cpt",
            filters={
                "solde": [">", "0"],
                "etat_cpte": 1,
                "id_prod": 7,
                "id_titulaire": account.id_titulaire,
            },
            fieldname=["num_complet_cpte", "solde"],
            order_by="id_titulaire asc",
            as_dict=1,
            # debug=True,
        )
        if not compulsory or len(compulsory) > 1:
            # log clients withtout compulsory
            continue
        num_compte_cible = compulsory[0].num_complet_cpte

        payload = {
            "identifiant_client": identifiant_client,
            "id_agence_source": "1",
            "id_compte_source": account.id_cpte,
            "id_agence_cible": "1",
            "num_compte_cible": num_compte_cible,
            "montant": account.solde,
            "libelle": "Try Transfer",
            "num_complet_compte_source": account.num_complet_cpte,
            "type_operation": "120",
        }

        response = requests.request(
            "POST",
            url,
            headers={
                "Authorization": 'WSSE profile="UsernameToken"',
                "X-WSSE": get_wsse(
                    "e0a662fab2544cde0a45e10c51f0e082",
                    "81481a008fdc9ab8d3eaa5029abac255",
                ),
            },
            data=payload,
        )

        # Log response
        json_response = response.json()

        # Get transfer Balances
        balances = frappe.cbs_db.get_values(
            "ad_cpt",
            filters={
                "etat_cpte": 1,
                "id_prod": ["in", [1, 7]],
                "id_titulaire": account.id_titulaire,
            },
            fieldname=["solde", "id_titulaire", "id_prod"],
            order_by="id_titulaire asc",
            as_dict=1,
            # debug=True,
        )

        from_bal = [x["solde"] for x in balances if x["id_prod"] == 1][0]
        to_bal = [x["solde"] for x in balances if x["id_prod"] == 7][0]

        logs.append(
            "transfer_logs",
            {
                "id_client": account.id_titulaire,
                "formatted_id_client": identifiant_client,
                "transfer_acct_no": account.num_complet_cpte,
                "transfer_account_id": account.id_cpte,
                "amount_to_transfer": account.solde,
                "balance_before_transfer": account.solde,
                "balance_after_transfer": from_bal,
                "receiving_account_id": num_compte_cible,
                "receiving_acct_balance_before_transfer": compulsory[0].solde,
                "receiving_acct_balance_after_transfer": to_bal,
                "request_body": json.dumps(payload),
                "transfer_api_success": 1 if json_response["success"] else 0,
                "transfer_api_code": response.status_code,
                "transfer_api_response": json.dumps(json_response),
            },
        )

        if json_response["success"]:
            total_account_transfered += 1
            total_transferred += account.solde

        # break

    total_after_transfer = frappe.cbs_db.get_values(
        "ad_cpt",
        filters={
            "solde": [">", "0"],
            "etat_cpte": 1,
            "id_prod": 1,
            "id_titulaire": ["not in", accounts_in_mob_banking],
        },
        fieldname=["sum(solde)"],
        as_dict=1,
        # debug=True,
    )

    # Get Total sum to be transferred
    comp_balance_after_transfer = frappe.cbs_db.get_values(
        "ad_cpt",
        filters={
            "solde": [">", "0"],
            "etat_cpte": 1,
            "id_prod": 7,
            "id_titulaire": ["not in", accounts_in_mob_banking],
        },
        fieldname=["sum(solde)"],
        as_dict=1,
        # debug=True,
    )

    logs.total_transferred = total_transferred
    logs.total_account_transfered = total_account_transfered
    logs.balance_after_transfer = total_after_transfer[0].sum
    logs.comp_balance_after_transfer = comp_balance_after_transfer[0].sum

    logs.epargne_diff = total_after_transfer[0].sum - total_to_transfer[0].sum
    logs.comp_diff = (
        comp_balance_after_transfer[0].sum - comp_balance_before_transfer[0].sum
    )
    logs.insert()

    root_conn.close()

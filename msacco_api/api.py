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
    root_conn = get_cbs_root_connection("victor", "123456")
    frappe.cbs_db = root_conn
    # print(root_conn.describe('ad_cli'))
    # print(root_conn.sql("SELECT * FROM ad_cli WHERE id_client = '10931'", as_dict=1))
    # print(len(root_conn.get_values("ad_cpt", filters={"solde": ["!=", "0"]}, fieldname=["*"], order_by="id_titulaire asc", as_dict=1)))
    # print(len(frappe.cbs_db.get_values("ad_cli")))

    # get all current accounts balance greater than 0

    # Accounts in Mobile Bankings have their balance already transferred
    accounts_in_mob_banking = frappe.cbs_db.get_values(
        "ad_abonnement",
        fieldname=["id_client"],
        order_by="id_client asc",
        as_dict=1,
        pluck="id_client",
    )

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
        debug=True,
    )

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
        debug=True,
    )

    print(
        # accounts_in_mob_banking,
        total_to_transfer,
        accounts_in_mob_banking,
        len(balances_to_transfer),
        len(accounts_in_mob_banking),
    )

    # Get Full Account No:

    url = "http://10.0.0.120/api/v1/client/transfert/compte"

    # Do the Transfer
    for account in balances_to_transfer:
        # Format ID Client to 8 digits with Agence ID
        identifiant_client = f'1{f"{account.id_titulaire}".zfill(ID_CLIENT_LENGTH)}'

        print(identifiant_client, account)

        # Get num Compulsory
        compulsory = frappe.cbs_db.get_values(
            "ad_cpt",
            filters={
                "solde": [">", "0"],
                "etat_cpte": 1,
                "id_prod": 2,
                "id_titulaire": account.id_titulaire,
            },
            fieldname=["num_complet_cpte"],
            order_by="id_titulaire asc",
            as_dict=1,
            debug=True,
            pluck="id_client",
        )
        print("??????", compulsory)
        if not compulsory or len(compulsory) > 1:
            # log clients withtout compulsory
            continue
        num_compte_cible = compulsory[0]

        payload = {
            "identifiant_client": identifiant_client,
            "id_agence_source": "1",
            "id_compte_source": account.id_cpte,
            "id_agence_cible": "1",
            "num_compte_cible": num_compte_cible,
            "montant": account.solde,
            "libelle": "Try Transfer",
            "num_complet_compte_source": account.num_complet_cpte,
            "type_operation": "25",
        }

        files = []
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
            files=files,
        )

        # Log response
        print(response.text)

        # Get transfer Balances
        balances = frappe.cbs_db.get_values(
            "ad_cpt",
            filters={
                "etat_cpte": 1,
                "id_prod": ["in", [1, 2]],
                "id_titulaire": account.id_titulaire,
            },
            fieldname=["solde", "id_titulaire"],
            order_by="id_titulaire asc",
            as_dict=1,
            debug=True,
        )

        # Log the Balances

        break

    root_conn.close()

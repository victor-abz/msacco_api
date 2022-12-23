# Copyright (c) 2022, Victor ABIZEYIMANA and contributors
# For license information, please see license.txt


import frappe


def execute(filters=None):
    columns = [
        {
            "fieldname": "id_client",
            "label": "ID Client",
            "fieldtype": "Data",
        },
        {
            "fieldname": "transfer_acct_no",
            "label": "Source Account",
            "fieldtype": "Data",
        },
        {
            "fieldname": "balance_before_transfer",
            "label": "Amount transferred",
            "fieldtype": "Data",
            "options": "currency",
        },
        {
            "fieldname": "receiving_account_id",
            "label": "Reciving Account ID",
            "fieldtype": "Data",
        },
        {
            "fieldname": "request_body",
            "label": "API Request Body",
            "fieldtype": "JSON",
        },
        {
            "fieldname": "transfer_api_response",
            "label": "API Response",
            "fieldtype": "JSON",
        },
		{
            "fieldname": "creation",
            "label": "Created Date",
            "fieldtype": "Date",
        },
    ]

    # "id_client": account.id_titulaire,
    #             "formatted_id_client": identifiant_client,
    #             "transfer_acct_no": account.num_complet_cpte,
    #             "transfer_account_id": account.id_cpte,
    #             "amount_to_transfer": account.solde,
    #             "balance_before_transfer": account.solde,
    #             "balance_after_transfer": from_bal,
    #             "receiving_account_id": num_compte_cible,
    #             "receiving_acct_balance_before_transfer": compulsory[0].solde,
    #             "receiving_acct_balance_after_transfer": to_bal,
    #             "request_body": json.dumps(payload),
    #             "transfer_api_success": 1 if json_response["success"] else 0,
    #             "transfer_api_code": response.status_code,
    #             "transfer_api_response": json.dumps(json_response),

    data = frappe.get_all("Transfer Logs", fields="*")

    return columns, data

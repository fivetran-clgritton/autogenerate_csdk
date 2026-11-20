import xml.etree.ElementTree as ET
import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

API_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"
PAGE_SIZE = 100


def schema(configuration: dict):
    return [
        {
            "table": "gl_accounts",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "ACCOUNTNO": "STRING",
                "TITLE": "STRING",
                "ACCOUNTTYPE": "STRING",
                "NORMALBALANCE": "STRING",
                "STATUS": "STRING",
                "CATEGORY": "STRING",
                "CLOSINGTYPE": "STRING",
                "WHENMODIFIED": "UTC_DATETIME",
                "WHENCREATED": "UTC_DATETIME",
            },
        },
        {
            "table": "gl_entries",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "JOURNAL": "STRING",
                "BATCH_DATE": "STRING",
                "BATCH_TITLE": "STRING",
                "REFERENCENO": "STRING",
                "ACCOUNTNO": "STRING",
                "AMOUNT": "FLOAT",
                "TR_TYPE": "STRING",
                "DOCNUMBER": "STRING",
                "DESCRIPTION": "STRING",
                "WHENMODIFIED": "UTC_DATETIME",
                "WHENCREATED": "UTC_DATETIME",
            },
        },
        {
            "table": "accounts_receivable_invoices",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "RECORDID": "STRING",
                "CUSTOMERID": "STRING",
                "CUSTOMERNAME": "STRING",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
                "DUEDATE": "STRING",
                "TOTALDUE": "FLOAT",
                "TOTALPAID": "FLOAT",
                "TOTALENTERED": "FLOAT",
                "STATE": "STRING",
                "CURRENCY": "STRING",
                "DESCRIPTION": "STRING",
            },
        },
        {
            "table": "accounts_payable_bills",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "RECORDID": "STRING",
                "VENDORID": "STRING",
                "VENDORNAME": "STRING",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
                "DUEDATE": "STRING",
                "TOTALDUE": "FLOAT",
                "TOTALPAID": "FLOAT",
                "TOTALENTERED": "FLOAT",
                "STATE": "STRING",
                "CURRENCY": "STRING",
                "DESCRIPTION": "STRING",
            },
        },
        {
            "table": "customers",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "CUSTOMERID": "STRING",
                "NAME": "STRING",
                "STATUS": "STRING",
                "CUSTTYPE": "STRING",
                "PHONE1": "STRING",
                "EMAIL1": "STRING",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
            },
        },
        {
            "table": "vendors",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "VENDORID": "STRING",
                "NAME": "STRING",
                "STATUS": "STRING",
                "VENDTYPE": "STRING",
                "PHONE1": "STRING",
                "EMAIL1": "STRING",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
            },
        },
        {
            "table": "employees",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "EMPLOYEEID": "STRING",
                "PERSONALINFO.FIRSTNAME": "STRING",
                "PERSONALINFO.LASTNAME": "STRING",
                "PERSONALINFO.EMAIL1": "STRING",
                "TITLE": "STRING",
                "STATUS": "STRING",
                "STARTDATE": "STRING",
                "ENDDATE": "STRING",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
            },
        },
        {
            "table": "projects",
            "primary_key": ["RECORDNO"],
            "columns": {
                "RECORDNO": "STRING",
                "PROJECTID": "STRING",
                "NAME": "STRING",
                "STATUS": "STRING",
                "PROJECTTYPE": "STRING",
                "CUSTOMERID": "STRING",
                "MANAGERID": "STRING",
                "BEGINDATE": "STRING",
                "ENDDATE": "STRING",
                "TOTALBUDGETED": "FLOAT",
                "WHENCREATED": "UTC_DATETIME",
                "WHENMODIFIED": "UTC_DATETIME",
            },
        },
    ]


def _build_request(cfg: dict, function_xml: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<request>
  <control>
    <senderid>{cfg["sender_id"]}</senderid>
    <password>{cfg["sender_password"]}</password>
    <controlid>fivetran-sync</controlid>
    <uniqueid>false</uniqueid>
    <dtdversion>3.0</dtdversion>
  </control>
  <operation>
    <authentication>
      <login>
        <userid>{cfg["user_id"]}</userid>
        <companyid>{cfg["company_id"]}</companyid>
        <password>{cfg["user_password"]}</password>
      </login>
    </authentication>
    <content>
      {function_xml}
    </content>
  </operation>
</request>"""


def _query_xml(object_name: str, fields: list, when_modified_min: str = None, offset: int = 0) -> str:
    filter_xml = ""
    if when_modified_min:
        filter_xml = f"""<filter>
          <greaterthanorequalto>
            <field>WHENMODIFIED</field>
            <value>{when_modified_min}</value>
          </greaterthanorequalto>
        </filter>"""

    fields_xml = "".join(f"<field>{f}</field>" for f in fields)
    return f"""<function controlid="q1">
      <query>
        <object>{object_name}</object>
        <select>{fields_xml}</select>
        {filter_xml}
        <pagesize>{PAGE_SIZE}</pagesize>
        <offset>{offset}</offset>
      </query>
    </function>"""


def _post(cfg: dict, function_xml: str) -> ET.Element:
    body = _build_request(cfg, function_xml)
    resp = requests.post(
        API_URL,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/xml"},
        timeout=60,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    # Check for API-level errors
    status = root.findtext(".//status")
    if status == "failure":
        errors = [e.text for e in root.findall(".//error/description2")]
        raise RuntimeError(f"Intacct API error: {'; '.join(errors)}")
    return root


def _extract_records(root: ET.Element, object_name: str) -> list:
    """Extract all child elements of each object record as a flat dict."""
    records = []
    for item in root.findall(f".//{object_name}"):
        row = {}
        for child in item:
            # Flatten one level of nesting (e.g. PERSONALINFO.FIRSTNAME)
            if len(child):
                for subchild in child:
                    row[f"{child.tag}.{subchild.tag}"] = subchild.text
            else:
                row[child.tag] = child.text
        if row:
            records.append(row)
    return records


def _sync_object(cfg, object_name, table_name, fields, when_modified_min=None):
    """Generator: yields op.upsert for each record, handles pagination."""
    offset = 0
    total = 0
    while True:
        fn_xml = _query_xml(object_name, fields, when_modified_min, offset)
        root = _post(cfg, fn_xml)
        records = _extract_records(root, object_name)
        for rec in records:
            yield op.upsert(table_name, rec)
        total += len(records)
        log.info(f"{table_name}: fetched {total} records")
        if len(records) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


def update(configuration: dict, state: dict):
    cfg = configuration
    modified_since = state.get("modified_since", "01/01/2010 00:00:00")

    # Full refresh: GL accounts
    yield from _sync_object(cfg, "GLACCOUNT",
        "gl_accounts",
        ["RECORDNO", "ACCOUNTNO", "TITLE", "ACCOUNTTYPE", "NORMALBALANCE",
         "STATUS", "CATEGORY", "CLOSINGTYPE", "WHENMODIFIED", "WHENCREATED"])

    # Incremental objects
    incremental = [
        ("GLENTRY", "gl_entries",
         ["RECORDNO", "JOURNAL", "BATCH_DATE", "BATCH_TITLE", "REFERENCENO",
          "ACCOUNTNO", "AMOUNT", "TR_TYPE", "DOCNUMBER", "DESCRIPTION",
          "WHENMODIFIED", "WHENCREATED"]),
        ("ARINVOICE", "accounts_receivable_invoices",
         ["RECORDNO", "RECORDID", "CUSTOMERID", "CUSTOMERNAME", "WHENCREATED",
          "WHENMODIFIED", "DUEDATE", "TOTALDUE", "TOTALPAID", "TOTALENTERED",
          "STATE", "CURRENCY", "DESCRIPTION"]),
        ("APBILL", "accounts_payable_bills",
         ["RECORDNO", "RECORDID", "VENDORID", "VENDORNAME", "WHENCREATED",
          "WHENMODIFIED", "DUEDATE", "TOTALDUE", "TOTALPAID", "TOTALENTERED",
          "STATE", "CURRENCY", "DESCRIPTION"]),
        ("CUSTOMER", "customers",
         ["RECORDNO", "CUSTOMERID", "NAME", "STATUS", "CUSTTYPE",
          "PHONE1", "EMAIL1", "WHENCREATED", "WHENMODIFIED"]),
        ("VENDOR", "vendors",
         ["RECORDNO", "VENDORID", "NAME", "STATUS", "VENDTYPE",
          "PHONE1", "EMAIL1", "WHENCREATED", "WHENMODIFIED"]),
        ("EMPLOYEE", "employees",
         ["RECORDNO", "EMPLOYEEID", "PERSONALINFO", "TITLE", "STATUS",
          "STARTDATE", "ENDDATE", "WHENCREATED", "WHENMODIFIED"]),
        ("PROJECT", "projects",
         ["RECORDNO", "PROJECTID", "NAME", "STATUS", "PROJECTTYPE",
          "CUSTOMERID", "MANAGERID", "BEGINDATE", "ENDDATE",
          "TOTALBUDGETED", "WHENCREATED", "WHENMODIFIED"]),
    ]

    for obj, table, fields in incremental:
        log.info(f"Syncing {table} modified since {modified_since}")
        yield from _sync_object(cfg, obj, table, fields, modified_since)

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    yield op.checkpoint(state={"modified_since": now})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()

"""
Azure Automation Runbook: sync_sf_it_cases   (10/5/2026, step 21d v2 - merged with telecare_sf_cases_sync)

The ONE Salesforce case sync for this project. Copies facility IT cases into Azure SQL so the web app,
the significance scan, the QBR app and the Slack feedback loop read Salesforce as the system of record.
READ-ONLY in Salesforce.

Replaces telecare_sf_cases_sync (step 24, daily 7:15 AM -> physiciantech.sf_cases). Same filter as that
runbook, so physiciantech.sf_cases (now a VIEW over sf_it_cases, step 21f) keeps returning the same cases:
    RecordTypeId = SF_RECORD_TYPE_ID (IT Helpdesk Ticket) AND AccountId != null
    AND Account.Name != 'TeleSpecialists, LLC'                    -> in_scope = 1, all categories
Systemic issues use only Category 'Client Support' (filtered in vw_sf_systemic_cases, not here).
Parent cases outside the filter are pulled too (in_scope = 0) so every systemic parent is present.

Writes: physiciantech.sf_it_cases and physiciantech.sf_it_case_history (Status / Owner / Parent changes).
Full reload each run in one transaction; refuses to wipe the tables if Salesforce returns nothing.

Automation Account: physicianrunbook. Variables: SF_CLIENT_ID, SF_CLIENT_SECRET, SF_LOGIN_URL,
SF_RECORD_TYPE_ID, PHYSICIANTECH_SQL_PASSWORD. Schedule: every 4 hours (and keep a run before 7:20 AM,
when significance_scan reads it).
"""
import logging
import sys
import time
from datetime import datetime, timezone

import automationassets
import pymssql
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout)  # stdout = Output tab

API = "v60.0"
SQL_SERVER = "tsitdatadev.database.windows.net"
SQL_DATABASE = "sqldb-it-operations-dev"
SQL_USER = "physiciantech_pipeline"
SQL_CONNECT_RETRY_DELAYS = [10, 20, 40, 60, 90, 120]
TRANSIENT_SQL_ERROR_CODES = {40613, 40197, 40501, 40540, 40143, 4060, 10928, 10929, 10053, 10054, 10060}
HISTORY_FIELDS = ("Status", "Owner", "Parent", "created")

CASE_FIELDS = ("Id, CaseNumber, ParentId, AccountId, Account.Name, Account.TS_Account_ID__c, Subject, "
               "Description, Category__c, Sub_Category__c, Status, IsClosed, Reason, Origin, Priority, "
               "Owner.Name, Escalate_to_Jira__c, Jira_Issue_Key__c, TC_Case_Number__c, Cart_Name__c, "
               "Resolution__c, CreatedDate, ClosedDate, LastModifiedDate")


def get_var(name):
    return automationassets.get_automation_variable(name)


# ---------------------------------------------------------------- Salesforce
def sf_session():
    login_url = get_var("SF_LOGIN_URL").rstrip("/")
    r = requests.post(f"{login_url}/services/oauth2/token", data={
        "grant_type": "client_credentials",
        "client_id": get_var("SF_CLIENT_ID"),
        "client_secret": get_var("SF_CLIENT_SECRET"),
    }, timeout=60)
    r.raise_for_status()
    t = r.json()
    return t["instance_url"].rstrip("/"), {"Authorization": f"Bearer {t['access_token']}"}


def soql_all(base, headers, q):
    """Run a SOQL query and follow nextRecordsUrl until every row is returned."""
    rows = []
    r = requests.get(f"{base}/services/data/{API}/query", headers=headers, params={"q": q}, timeout=120)
    while True:
        r.raise_for_status()
        body = r.json()
        rows.extend(body.get("records", []))
        nxt = body.get("nextRecordsUrl")
        if not nxt:
            return rows
        r = requests.get(f"{base}{nxt}", headers=headers, timeout=120)


def sf_dt(value):
    """'2026-10-05T12:30:20.000+0000' -> naive UTC datetime."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone.utc).replace(tzinfo=None)


def cut(value, n):
    if value is None:
        return None
    s = str(value)
    return s[:n]


def case_row(c, in_scope):
    acct = c.get("Account") or {}
    owner = c.get("Owner") or {}
    return (
        c["Id"], c["CaseNumber"], c.get("ParentId"), c.get("AccountId"),
        cut(acct.get("Name"), 255), cut(acct.get("TS_Account_ID__c"), 50),
        cut(c.get("Subject"), 255), cut(c.get("Description"), 4000),
        cut(c.get("Category__c"), 100), cut(c.get("Sub_Category__c"), 100),
        cut(c.get("Status"), 50), 1 if c.get("IsClosed") else 0,
        cut(c.get("Reason"), 100), cut(c.get("Origin"), 50), cut(c.get("Priority"), 20),
        cut(owner.get("Name"), 255), 1 if c.get("Escalate_to_Jira__c") else 0,
        cut(c.get("Jira_Issue_Key__c"), 50), cut(c.get("TC_Case_Number__c"), 30),
        cut(c.get("Cart_Name__c"), 50), cut(c.get("Resolution__c"), 4000),
        sf_dt(c.get("CreatedDate")), sf_dt(c.get("ClosedDate")), sf_dt(c.get("LastModifiedDate")),
        in_scope,
    )


def fetch(base, headers, rt_id):
    where = (f"RecordTypeId = '{rt_id}' AND AccountId != null "
             f"AND Account.Name != 'TeleSpecialists, LLC'")
    cases = soql_all(base, headers, f"SELECT {CASE_FIELDS} FROM Case WHERE {where}")
    client_support = sum(1 for c in cases if c.get("Category__c") == "Client Support")
    logging.info(f"In-scope cases from Salesforce: {len(cases)} ({client_support} Client Support)")
    ids = {c["Id"] for c in cases}

    # Parent cases that fall outside the filter (e.g. a parent on another category) - pull them too
    missing = sorted({c["ParentId"] for c in cases if c.get("ParentId") and c["ParentId"] not in ids})
    parents = []
    for i in range(0, len(missing), 200):
        chunk = "','".join(missing[i:i + 200])
        parents.extend(soql_all(base, headers, f"SELECT {CASE_FIELDS} FROM Case WHERE Id IN ('{chunk}')"))
    logging.info(f"Out-of-scope parent cases pulled: {len(parents)}")

    all_ids = sorted(ids | {p["Id"] for p in parents})
    history = []
    fields = "','".join(HISTORY_FIELDS)
    for i in range(0, len(all_ids), 200):
        chunk = "','".join(all_ids[i:i + 200])
        history.extend(soql_all(base, headers,
            "SELECT Id, CaseId, Field, OldValue, NewValue, CreatedBy.Name, CreatedDate FROM CaseHistory "
            f"WHERE CaseId IN ('{chunk}') AND Field IN ('{fields}')"))
    logging.info(f"History rows: {len(history)}")

    case_rows = [case_row(c, 1) for c in cases] + [case_row(p, 0) for p in parents]
    hist_rows = [(h["Id"], h["CaseId"], cut(h["Field"], 50), cut(h.get("OldValue"), 255),
                  cut(h.get("NewValue"), 255), cut((h.get("CreatedBy") or {}).get("Name"), 255),
                  sf_dt(h["CreatedDate"])) for h in history]
    return case_rows, hist_rows


# ---------------------------------------------------------------- SQL
def get_sql_connection():
    last_error = None
    for attempt, delay in enumerate([0] + SQL_CONNECT_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            return pymssql.connect(server=SQL_SERVER, port=1433, database=SQL_DATABASE, user=SQL_USER,
                                   password=get_var("PHYSICIANTECH_SQL_PASSWORD"))
        except pymssql.OperationalError as exc:
            last_error = exc
            code = exc.args[0] if exc.args else None
            if isinstance(code, tuple):          # pymssql nests (number, message) inside args
                code = code[0] if code else None
            if code not in TRANSIENT_SQL_ERROR_CODES:
                raise
            logging.warning(f"SQL connect failed (error {code}, attempt {attempt + 1}) - will retry")
    raise last_error


def load(case_rows, hist_rows):
    conn = get_sql_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM physiciantech.sf_it_case_history")
        cur.execute("DELETE FROM physiciantech.sf_it_cases")
        cur.executemany(
            "INSERT INTO physiciantech.sf_it_cases (case_id, case_number, parent_id, account_id, account_name, "
            "ts_account_id, subject, description, category, sub_category, status, is_closed, reason, origin, "
            "priority, owner_name, escalate_to_jira, jira_issue_key, tc_case_number, cart_name, resolution, "
            "created_at_utc, closed_at_utc, modified_at_utc, in_scope) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            case_rows)
        if hist_rows:
            cur.executemany(
                "INSERT INTO physiciantech.sf_it_case_history (history_id, case_id, field, old_value, new_value, "
                "changed_by, changed_at_utc) VALUES (%s,%s,%s,%s,%s,%s,%s)", hist_rows)
        conn.commit()
        cur.execute("SELECT COUNT(*), SUM(CASE WHEN parent_id IS NOT NULL THEN 1 ELSE 0 END), "
                    "SUM(CASE WHEN is_closed = 0 THEN 1 ELSE 0 END) FROM physiciantech.sf_it_cases")
        n, with_parent, open_ = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM physiciantech.vw_sf_systemic_cases")
        systemic = cur.fetchone()[0]
        logging.info(f"Loaded: {n} cases ({open_} open, {with_parent} with a parent case), "
                     f"{len(hist_rows)} history rows, {systemic} systemic parent cases")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    rt_id = get_var("SF_RECORD_TYPE_ID")
    base, headers = sf_session()
    logging.info(f"Salesforce {base}, record type {rt_id}")
    case_rows, hist_rows = fetch(base, headers, rt_id)
    if not case_rows:
        raise RuntimeError("Salesforce returned 0 in-scope cases - not wiping the SQL tables")
    load(case_rows, hist_rows)
    logging.info("sync_sf_it_cases done")


main()
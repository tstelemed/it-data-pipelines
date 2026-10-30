"""
sync_physicians.py — Azure Automation Runbook (Python)

PURPOSE
-------
Populates physiciantech.physicians from DOMO's "Salesforce | Physician
Timeline" dataset, so this project has a local, queryable physician
table for future Slack-integration lookups (e.g. "who covers Neuro
Stroke at Facility X").

WHY DOMO, NOT SALESFORCE DIRECTLY (read before touching this file)
--------------------------------------------------------------------
A PowerShell runbook (sync_physicians.ps1) was built first, querying
Salesforce Contact directly via the same OAuth Connected App already
used for Case creation. It correctly authenticated and had the right
object/field-level permissions, but returned zero rows in production
despite the identical query returning 264 rows when run with broader
access - the signature of Contact's Organization-Wide Default (or a
missing sharing rule) silently filtering every row out for that specific
integration user, not a permissions error. Fixing that would require an
org-wide Salesforce sharing-model change, which needs the Salesforce
security team's sign-off before touching - correctly not pursued
unilaterally.

DOMO already has this same underlying data synced via its own Salesforce
connector (which has broader access than our narrowly-scoped
integration user), so reading it from DOMO sidesteps the sharing problem
entirely rather than routing around it. This also settles the
"PowerShell vs Python" question from that abandoned attempt: the only
reason for PowerShell was reusing the Case-creation script's Salesforce
OAuth code. That reason no longer applies with DOMO as the source, and
DOMO sync is already this project's established Python pattern
(sync_facility_lookup.py) - Python here is the natural fit, not a
stylistic choice.

THE DEDUP PROBLEM, AND THE CAVEAT THAT COMES WITH ITS FIX
------------------------------------------------------------
Salesforce | Physician Timeline has one row per physician CONTRACT, not
one row per physician (confirmed directly against its live data, 9/25 -
e.g. two physicians with 2 rows each, same Id, different
Signed_Contract_Date/Physician_Contract_Status). The reliable signal for
"which row is this physician's current one" is Physician Contract
Status = 'Activated' - a plain passthrough field from the underlying
Salesforce | Physician dataset, untouched by anything else in this
pipeline. That's what dedup_physicians() applies: prefer 'Activated',
tie-break by most recent Signed Contract Date for anyone with no
'Activated' row (e.g. someone who's fully departed).

Separately - confirmed directly against the source ETL's own SQL, not
inferred - three fields (Credentialing Status, Go-Live Date, TS
Resignation Date) pass through a join in that ETL that pairs each
physician's contract-history rows with their resignation-history events
BY POSITIONAL INDEX ("Contract_Number" = "Timeline Number"), not by any
real correspondence between the two. A physician's current/Activated
contract row can end up paired with a resignation/go-live event that
doesn't actually belong to that contract. These three fields are synced
as-is (still more useful present than absent), but should be treated as
lower-confidence context, not verified fact - see the column comments in
physicians_table_v2.sql for the same caveat at the schema level.

REQUIRED AZURE AUTOMATION VARIABLES (all should already exist - reused
from sync_facility_lookup.py, none new)
------------------------------------------------------------------------
DOMO_CLIENT_ID              Same DOMO client already used for TC | Facilities
DOMO_CLIENT_SECRET
PHYSICIANTECH_SQL_PASSWORD  Password for the physiciantech_pipeline SQL login

FIRST RUN — READ THIS BEFORE TRUSTING THE OUTPUT
--------------------------------------------------
Following the same discipline as sync_facility_lookup.py: this script
logs DOMO's actual returned column names and checks them against
EXPECTED_COLUMNS before writing anything. The column names below were
confirmed directly against live DOMO query results (9/25), not assumed -
firmer footing than that script's original version had - but still
worth a glance at the first run's log line regardless, since a live
DOMO query can behave differently than a point-in-time check.
"""

import time
from datetime import datetime, timezone

import automationassets
import pymssql
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DOMO_TOKEN_URL = "https://api.domo.com/oauth/token"
DOMO_QUERY_URL_TEMPLATE = "https://api.domo.com/v1/datasets/query/execute/{dataset_id}"
PHYSICIAN_TIMELINE_DATASET_ID = "7f936ebb-cc19-42e2-a37f-4c12aeb3f236"

SQL_SERVER = "tsitdatadev.database.windows.net"
SQL_DATABASE = "sqldb-it-operations-dev"
SQL_USER = "physiciantech_pipeline"

# Same retry discipline established after the 9/8-9/9 auto-pause
# incidents elsewhere in this project - kept identical rather than
# reinvented.
TRANSIENT_SQL_ERROR_CODES = {
    40613, 40197, 40501, 40540, 40143, 4060, 10928, 10929, 10053, 10054, 10060,
}
SQL_CONNECT_RETRY_DELAYS = [10, 20, 40, 60, 90, 120]

# Confirmed directly against live DOMO query results (9/25) - see the
# module docstring. Logged and checked before use, same discipline as
# sync_facility_lookup.py.
EXPECTED_COLUMNS = [
    "Id", "Full Name", "FirstName", "LastName", "Physician", "Email", "TS Email",
    "Mobile", "NPI Number", "Specialty", "Sub Specialty", "Provider Type",
    "Degree", "EEG Reads", "Employment Status", "Physician Liaison", "Account",
    "Physician Contract Status", "Physician Inactive", "Signed Contract Date",
    "Credentialing Status", "Go-Live Date", "TS Resignation Date",
]

# FIXED (9/25) - this used to be a server-side dedup query using
# ROW_NUMBER() OVER (...) with a subquery. Confirmed via a live job run
# that DOMO's dataset QUERY API (this endpoint) rejects it with a 400 -
# "Underlying service error: Internal Server Error" - even though the
# identical window-function syntax works fine inside DOMO's own ETL/
# dataflow builder (which is a different execution engine from this
# simpler query API). Reverted to a plain SELECT with no window
# functions or subqueries - the same shape query that already works for
# sync_facility_lookup.py - and the dedup logic moved into Python
# instead (see dedup_physicians below). Plain fetch + local logic is
# also just easier to test and debug than trusting a remote SQL dialect
# to do something clever - the same lesson the abandoned PowerShell/
# Salesforce attempt already reinforced a few times over.
SELECT_SQL = "SELECT * FROM `{dataset_id}`".format(dataset_id=PHYSICIAN_TIMELINE_DATASET_ID)


def get_var(name: str) -> str:
    return automationassets.get_automation_variable(name)


# ---------------------------------------------------------------------------
# DOMO
# ---------------------------------------------------------------------------

def get_domo_token() -> str:
    client_id = get_var("DOMO_CLIENT_ID")
    client_secret = get_var("DOMO_CLIENT_SECRET")
    resp = requests.post(
        DOMO_TOKEN_URL,
        params={"grant_type": "client_credentials", "scope": "data"},
        auth=(client_id, client_secret),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_physicians_from_domo(token: str) -> list[dict]:
    url = DOMO_QUERY_URL_TEMPLATE.format(dataset_id=PHYSICIAN_TIMELINE_DATASET_ID)
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"sql": SELECT_SQL},
    )
    if not resp.ok:
        # Same discipline as every DOMO call in this project: always log
        # the actual response body on failure, never just raise_for_status()
        # alone.
        print(f"DOMO query failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    body = resp.json()
    columns = body.get("columns", [])
    rows = body.get("rows", [])

    print(f"DOMO returned columns: {columns}")
    missing = [c for c in EXPECTED_COLUMNS if c not in columns]
    if missing:
        raise RuntimeError(
            f"Salesforce | Physician Timeline is missing expected column(s) "
            f"{missing} - got {columns} instead. Not proceeding with a write "
            f"based on an unconfirmed column mapping."
        )

    col_index = {name: i for i, name in enumerate(columns)}
    results = []
    for row in rows:
        results.append({col: row[idx] for col, idx in col_index.items()})
    return results


def dedup_physicians(rows: list[dict]) -> list[dict]:
    """
    NEW (9/25) - the dedup logic this project already validated (prefer
    Physician Contract Status = 'Activated'; tie-break by most recent
    Signed Contract Date for anyone with no Activated row at all), now
    done in Python instead of DOMO SQL - see the note on SELECT_SQL
    above for why. One pass, grouping by Id, then picking the best row
    per group according to the same priority/tie-break rule the SQL
    version used.
    """
    def sort_key(row):
        is_activated = row.get("Physician Contract Status") == "Activated"
        contract_date = row.get("Signed Contract Date") or ""
        # False sorts before True, so "not is_activated" puts Activated
        # rows first when this tuple is used as a MINIMUM key below.
        return (not is_activated, contract_date)

    best_by_id: dict[str, dict] = {}
    for row in rows:
        physician_id = row.get("Id")
        if not physician_id:
            continue
        current_best = best_by_id.get(physician_id)
        if current_best is None:
            best_by_id[physician_id] = row
            continue
        # Prefer Activated over anything else; among non-Activated (or
        # among two Activated - shouldn't happen, but handled the same
        # way regardless), prefer the most recent Signed Contract Date.
        current_is_activated = current_best.get("Physician Contract Status") == "Activated"
        new_is_activated = row.get("Physician Contract Status") == "Activated"
        if new_is_activated and not current_is_activated:
            best_by_id[physician_id] = row
        elif new_is_activated == current_is_activated:
            current_date = current_best.get("Signed Contract Date") or ""
            new_date = row.get("Signed Contract Date") or ""
            if new_date > current_date:
                best_by_id[physician_id] = row
        # else: new row is non-Activated and current best is Activated -
        # keep the current best, do nothing.

    return list(best_by_id.values())


def parse_bool(value) -> bool | None:
    """DOMO returns booleans as literal 'true'/'false' strings, not a
    native type - confirmed elsewhere in this project (Facility Active on
    TC | Facilities) and true here too for Physician Inactive."""
    if value is None:
        return None
    return str(value).strip().lower() == "true"


def parse_date(value):
    if value is None or str(value).strip() == "":
        return None
    return value  # pymssql accepts ISO-format date strings directly


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def get_sql_connection():
    password = get_var("PHYSICIANTECH_SQL_PASSWORD")
    last_error = None
    for attempt, delay in enumerate([0] + SQL_CONNECT_RETRY_DELAYS, start=1):
        if delay:
            print(f"SQL connect attempt {attempt}/{len(SQL_CONNECT_RETRY_DELAYS) + 1} - waiting {delay}s before retry")
            time.sleep(delay)
        try:
            return pymssql.connect(
                server=SQL_SERVER,
                port=1433,
                database=SQL_DATABASE,
                user=SQL_USER,
                password=password,
            )
        except pymssql.Error as exc:
            last_error = exc
            code = None
            if exc.args and isinstance(exc.args[0], tuple) and len(exc.args[0]) >= 1:
                code = exc.args[0][0]
            transient = code in TRANSIENT_SQL_ERROR_CODES
            print(f"SQL connect attempt {attempt} failed (code={code}, transient={transient}): {exc}")
            if not transient:
                raise
    raise last_error


def replace_physicians_table(conn, rows: list[dict]) -> None:
    """
    DELETE FROM, not TRUNCATE TABLE - the same lesson already learned the
    hard way on sync_facility_lookup.py: TRUNCATE requires ALTER
    permission, which physiciantech_pipeline's schema-level grant doesn't
    cover (DELETE does). Full clear + reload in one transaction, same
    "Replace" pattern as that script - reasonable at this table's size,
    and nothing else reads this table mid-transaction.
    """
    cursor = conn.cursor()
    cursor.execute("DELETE FROM physiciantech.physicians")
    if rows:
        now = datetime.now(timezone.utc)
        cursor.executemany(
            "INSERT INTO physiciantech.physicians "
            "(sf_contact_id, full_name, first_name, last_name, display_name, "
            "email, ts_email, mobile, npi_number, specialty, sub_specialty, "
            "provider_type, degree, eeg_reads, employment_status, physician_liaison, "
            "account_name, physician_contract_status, physician_inactive, "
            "signed_contract_date, credentialing_status, go_live_date, "
            "ts_resignation_date, last_synced_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r["Id"],
                    r["Full Name"] or None,
                    r["FirstName"] or None,
                    r["LastName"] or None,
                    r["Physician"] or None,
                    r["Email"] or None,
                    r["TS Email"] or None,
                    r["Mobile"] or None,
                    r["NPI Number"] or None,
                    r["Specialty"] or None,
                    r["Sub Specialty"] or None,
                    r["Provider Type"] or None,
                    r["Degree"] or None,
                    r["EEG Reads"] or None,
                    r["Employment Status"] or None,
                    r["Physician Liaison"] or None,
                    r["Account"] or None,
                    r["Physician Contract Status"] or None,
                    parse_bool(r["Physician Inactive"]),
                    parse_date(r["Signed Contract Date"]),
                    r["Credentialing Status"] or None,
                    parse_date(r["Go-Live Date"]),
                    parse_date(r["TS Resignation Date"]),
                    now,
                )
                for r in rows
            ],
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    token = get_domo_token()
    all_rows = get_physicians_from_domo(token)
    print(f"Fetched {len(all_rows)} raw physician-contract row(s) from DOMO")

    physicians = dedup_physicians(all_rows)
    print(f"Deduplicated to {len(physicians)} physician(s) (one row per Id)")

    skipped = [p for p in physicians if not p["Id"]]
    if skipped:
        print(f"WARNING: {len(skipped)} row(s) had a blank Id and will be skipped")
    usable = [p for p in physicians if p["Id"]]

    conn = get_sql_connection()
    try:
        replace_physicians_table(conn, usable)
        print(f"Wrote {len(usable)} rows to physiciantech.physicians")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
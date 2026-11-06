"""
sync_facility_lookup.py - Azure Automation Runbook (Python)

PURPOSE
-------
Rebuilds physiciantech.facility_master (the facility master table) and
physiciantech.facility_carts from DOMO's "TC | Facilities" dataset, once a day.

facility_master holds one row per facility: facility_key, facility_name,
ts_account_id, health_system, facility_active, video software and cart names.
CLEAR (the web app), significance_scan, client_issue_summaries,
ticket_case_corrections and the facility views (vw_facility_watch_list,
vw_facility_issues, vw_facility_password_policy) read it.

RENAMED 11/6/2026 (John): the table was called power_app_facility_lookup
because it was first built for a Power App dropdown. The Power App is retired,
so the table is now facility_master. A view named power_app_facility_lookup was
left in place during the move and is dropped once every reader uses the new name.
Docstring rewritten 11/6; logic unchanged.

Keyed on facility_key (TC | Facilities' own "Facility Key" column), not on
facility name, which this project found has a ~3.8% mismatch rate.

SCHEDULE
--------
Daily at 4:00 AM Eastern (schedule "facilitiy_sync" in the physicianrunbook
Automation account). If it reads stale data, check when TC | Facilities itself
finishes refreshing in DOMO.

REQUIRED AUTOMATION VARIABLES (all already exist in this Automation Account)
---------------------------------------------------------------------------
DOMO_CLIENT_ID              DOMO client, same as telecare_classify_crosswalk.
DOMO_CLIENT_SECRET
PHYSICIANTECH_SQL_PASSWORD  Password for the physiciantech_pipeline SQL login
                            (server/database/user are constants below).

CHECKS
------
The script logs DOMO's returned column names and stops, without writing, if
any expected column is missing. "Facility Active" arrives as the text
"true"/"false" and is parsed explicitly.
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
TC_FACILITIES_DATASET_ID = "22d03892-90fc-4124-9217-fa212bce375d"

SQL_SERVER = "tsitdatadev.database.windows.net"
SQL_DATABASE = "sqldb-it-operations-dev"
SQL_USER = "physiciantech_pipeline"

# Same retry discipline already established in submit/poll/crosswalk
# after the 9/8-9/9 auto-pause incidents - kept identical rather than
# reinvented, since this script hits the same database under the same
# failure modes.
TRANSIENT_SQL_ERROR_CODES = {
    40613, 40197, 40501, 40540, 40143, 4060, 10928, 10929, 10053, 10054, 10060,
}
SQL_CONNECT_RETRY_DELAYS = [10, 20, 40, 60, 90, 120]

# What this script expects DOMO to call these columns. Logged and
# checked against the real response before use - see docstring above.
# Confirmed 9/24 against a real TC | Facilities CSV export - the
# original five names match exactly, including "Facility Active"
# (values are the literal strings "true"/"false", not a native boolean).
#
# ADDED (9/25) - the 8 Cart/Video Software columns. These were left out
# originally because a multi-session investigation couldn't confirm
# they were even queryable - every attempt through the DOMO MCP tool
# silently failed on any column name ending in a trailing digit. Since
# confirmed (via DOMO's own SQL tile, not this tool) to be a limitation
# specific to that MCP layer, not the data or DOMO itself - this script
# talks to DOMO's Data API directly, the same execution path as the SQL
# tile that worked, so there's no reason to expect the same issue here.
EXPECTED_COLUMNS = [
    "Facility Key", "Facility", "TS Account ID", "Health System", "Facility Active",
    "Video Software", "Cart Numbers", "Video Software 1", "Cart Number 1",
    "Video Software 2", "Cart Number 2", "Video Software 3", "Cart Number 3",
]


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


def parse_cart_names(raw) -> list[str]:
    """
    Splits the pipe-delimited Cart Numbers string into individual cart
    names, one entry per cart for facility_carts. Strips whitespace
    around each segment (source data has inconsistent spacing - e.g.
    "CSMH FSED " with a trailing space) and drops any empty segments (a
    leading/trailing pipe would otherwise produce a blank entry).
    """
    if not raw:
        return []
    return [seg.strip() for seg in str(raw).split("|") if seg.strip()]


def get_facilities_from_domo(token: str) -> list[dict]:
    sql = (
        "SELECT `Facility Key`, `Facility`, `TS Account ID`, `Health System`, "
        "`Facility Active`, `Video Software`, `Cart Numbers`, `Video Software 1`, "
        "`Cart Number 1`, `Video Software 2`, `Cart Number 2`, `Video Software 3`, "
        "`Cart Number 3` "
        "FROM table"
    )
    url = DOMO_QUERY_URL_TEMPLATE.format(dataset_id=TC_FACILITIES_DATASET_ID)
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"sql": sql},
    )
    if not resp.ok:
        # Same discipline as every DOMO call in this project: always log
        # the actual response body on failure, never just raise_for_status()
        # alone - the body usually names the real problem (bad column name,
        # bad dataset id) that the generic HTTP error hides.
        print(f"DOMO query failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    body = resp.json()
    columns = body.get("columns", [])
    rows = body.get("rows", [])

    print(f"DOMO returned columns: {columns}")
    missing = [c for c in EXPECTED_COLUMNS if c not in columns]
    if missing:
        raise RuntimeError(
            f"TC | Facilities is missing expected column(s) {missing} - "
            f"got {columns} instead. Not proceeding with a write based on "
            f"an unconfirmed column mapping. Fix EXPECTED_COLUMNS / the "
            f"SELECT above to match, then re-run."
        )

    col_index = {name: i for i, name in enumerate(columns)}
    results = []
    for row in rows:
        # DOMO returns "Facility Active" as the literal string "true"/
        # "false" (confirmed against a real export, 9/24), not a native
        # boolean - parse explicitly rather than relying on Python
        # truthiness, since any non-empty string (including "false")
        # would otherwise evaluate truthy.
        active_raw = row[col_index["Facility Active"]]
        facility_active = str(active_raw).strip().lower() == "true" if active_raw is not None else None
        results.append(
            {
                "facility_key": row[col_index["Facility Key"]],
                "facility_name": row[col_index["Facility"]],
                "ts_account_id": row[col_index["TS Account ID"]],
                "health_system": row[col_index["Health System"]],
                "facility_active": facility_active,
                "video_software": row[col_index["Video Software"]],
                "cart_numbers": row[col_index["Cart Numbers"]],
                "video_software_1": row[col_index["Video Software 1"]],
                "cart_number_1": row[col_index["Cart Number 1"]],
                "video_software_2": row[col_index["Video Software 2"]],
                "cart_number_2": row[col_index["Cart Number 2"]],
                "video_software_3": row[col_index["Video Software 3"]],
                "cart_number_3": row[col_index["Cart Number 3"]],
            }
        )
    return results


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


def replace_facility_lookup(conn, rows: list[dict]) -> None:
    """
    Small reference table (~617 rows) - full clear + reload in one
    transaction is simpler and safer than incremental upsert logic for
    something this size, and matches the "Replace" philosophy this
    project already used for the old Occurrences Excel sync. Nothing
    reads this table mid-transaction (it's a reference table read by
    CLEAR and the runbooks, and nothing else in this pipeline writes to it), so a brief
    moment of empty table during the transaction is not a real risk in
    practice, and the transaction guarantees it's never visible anyway.

    FIXED (9/24) - was TRUNCATE TABLE, which requires ALTER permission
    on the table, not just DELETE. physiciantech_pipeline had DELETE
    (from the schema-level grant) but not ALTER, so every run failed
    with a permissions error even though the DML grants were correct -
    a different, more specific permission than the ones already granted.
    DELETE FROM needs no new grant at all and is functionally identical
    for a table this size.

    CHANGED (9/25) - now also rebuilds facility_carts (one row per
    facility per cart, split from the raw pipe-delimited Cart Numbers
    string). facility_carts has a foreign key back to this table's
    facility_key, which fixes the DELETE/INSERT order: child table
    deleted BEFORE the parent (or the parent delete would fail on the
    FK constraint with child rows still referencing it), and parent
    inserted BEFORE the child (or the child insert would fail with
    nothing yet to reference). Everything in one transaction - a
    mid-run failure shouldn't leave the two tables inconsistent with
    each other.
    """
    cursor = conn.cursor()
    cursor.execute("DELETE FROM physiciantech.facility_carts")
    cursor.execute("DELETE FROM physiciantech.facility_master")
    if rows:
        cursor.executemany(
            "INSERT INTO physiciantech.facility_master "
            "(facility_key, facility_name, ts_account_id, health_system, facility_active, "
            "video_software, cart_numbers, video_software_1, cart_number_1, "
            "video_software_2, cart_number_2, video_software_3, cart_number_3, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r["facility_key"],
                    r["facility_name"],
                    r["ts_account_id"] or None,
                    r["health_system"] or None,
                    r["facility_active"],
                    r["video_software"] or None,
                    r["cart_numbers"] or None,
                    r["video_software_1"] or None,
                    r["cart_number_1"] or None,
                    r["video_software_2"] or None,
                    r["cart_number_2"] or None,
                    r["video_software_3"] or None,
                    r["cart_number_3"] or None,
                    datetime.now(timezone.utc),
                )
                for r in rows
            ],
        )

        cart_rows = []
        now = datetime.now(timezone.utc)
        for r in rows:
            for position, cart_name in enumerate(parse_cart_names(r["cart_numbers"]), start=1):
                cart_rows.append((r["facility_key"], position, cart_name, now))
        if cart_rows:
            cursor.executemany(
                "INSERT INTO physiciantech.facility_carts "
                "(facility_key, cart_position, cart_name, updated_at) "
                "VALUES (%s, %s, %s, %s)",
                cart_rows,
            )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    token = get_domo_token()
    facilities = get_facilities_from_domo(token)
    print(f"Fetched {len(facilities)} facilities from TC | Facilities")

    skipped = [f for f in facilities if not f["facility_key"]]
    if skipped:
        print(f"WARNING: {len(skipped)} row(s) had a blank Facility Key and will be skipped")
    usable = [f for f in facilities if f["facility_key"]]

    conn = get_sql_connection()
    try:
        replace_facility_lookup(conn, usable)
        print(f"Wrote {len(usable)} rows to physiciantech.facility_master")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
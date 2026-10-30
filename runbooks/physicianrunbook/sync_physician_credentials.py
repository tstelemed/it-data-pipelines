#!/usr/bin/env python
#!/usr/bin/env python
"""
Azure Automation Runbook: sync_physician_credentials
====================================================================
NEW 10/7/2026 - Physician facility-account lockouts (step 26, IT Pareto project).

PURPOSE: which physicians are credentialed at which facility. The lockout views
(step 23b/23d) only knew about physicians who had a consult at a facility since
7/1/2026. Credentialed physicians with no recent consult - the ones most likely
to be locked out - were missing. This runbook loads the credential list so the
step 26b views can include them.

SOURCE: DOMO "TC | SF | Credentials & Facilities Details"
(d5f26628-1413-437f-9507-e97ef3ef5014). One row per physician x facility, with
the credential From / To dates. Current credentials only (checked 10/7: min To
is 10/9/2026), unique on NPI x TS Account ID (1 duplicate in 59,044 - first kept).

WHAT IT DOES: full reload of physiciantech.physician_facility_credentials
(DELETE all + INSERT, one transaction). If DOMO returns nothing, the table is
left untouched. Safe to re-run.

All specialties are stored (Neurology, Psychiatry, Nurse Practitioner). The
views limit to Neurology, because consult history (TC | Cases) is neuro only.

Required one-time infra: run step26a_physician_facility_credentials.sql first.
Required Automation Variables (all already exist): TELECARE_DOMO_CLIENT_ID,
TELECARE_DOMO_CLIENT_SECRET, PHYSICIANTECH_SQL_PASSWORD.
Runtime environment: python310-classify-pipeline. Suggested schedule: daily
4:40 AM (after sync_consult_volume at 4:30).
"""

import logging
import sys
import time
from collections import Counter

import requests
import pymssql
import automationassets

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout)

CRED_DATASET_ID = "d5f26628-1413-437f-9507-e97ef3ef5014"  # TC | SF | Credentials & Facilities Details

SQL_SERVER = "tsitdatadev.database.windows.net"
SQL_DATABASE = "sqldb-it-operations-dev"
SQL_USER = "physiciantech_pipeline"

SQL_CONNECT_RETRY_DELAYS = [10, 20, 40, 60, 90, 120]
TRANSIENT_SQL_ERROR_CODES = {40613, 40197, 40501, 40540, 40143, 4060, 10928, 10929, 10053, 10054, 10060}

N_COLS = 12


def get_var(name: str) -> str:
    return automationassets.get_automation_variable(name)


# ---------------------------------------------------------------------------
# DOMO
# ---------------------------------------------------------------------------

def get_domo_access_token() -> str:
    resp = requests.post(
        "https://api.domo.com/oauth/token",
        params={"grant_type": "client_credentials", "scope": "data"},
        auth=(get_var("TELECARE_DOMO_CLIENT_ID"), get_var("TELECARE_DOMO_CLIENT_SECRET")),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def domo_query_dataset(token: str, dataset_id: str, sql: str) -> list[dict]:
    resp = requests.post(
        f"https://api.domo.com/v1/datasets/query/execute/{dataset_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"sql": sql},
        timeout=300,
    )
    if not resp.ok:
        logging.error(f"DOMO query failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
    body = resp.json()
    logging.info(f"DOMO returned columns: {body['columns']}")
    return [dict(zip(body["columns"], row)) for row in body["rows"]]


def fetch_credentials(token: str) -> list[dict]:
    sql = f"""SELECT `TS Account ID`, `NPI Number`, `Physician`, `Specialty`, `Employment Status`,
                     `Facility`, `Health System`, `Division`,
                     `Credentialing Entity`, `Credentialing Entity ID`, `From`, `To`
              FROM `{CRED_DATASET_ID}`
              WHERE `Physician` IS NOT NULL AND `Physician` <> ''"""
    return domo_query_dataset(token, CRED_DATASET_ID, sql)


# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

def _npi(v):
    """NPI comes back as a number (e.g. 1558724799 or 1558724799.0) - plain 10-digit string, else None."""
    s = str(v or "").strip()
    if not s:
        return None
    try:
        s = str(int(float(s)))
    except (TypeError, ValueError):
        pass
    return s if s.isdigit() and len(s) == 10 else None


def _date(v):
    s = str(v or "").strip()
    return s[:10] if len(s) >= 10 else None


def _txt(v, n):
    s = str(v or "").strip()
    return s[:n] or None


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def get_sql_connection():
    last_error = None
    for attempt, delay in enumerate([0] + SQL_CONNECT_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            return pymssql.connect(
                server=SQL_SERVER, port=1433, database=SQL_DATABASE, user=SQL_USER,
                password=get_var("PHYSICIANTECH_SQL_PASSWORD"),
            )
        except pymssql.OperationalError as exc:
            last_error = exc
            error_code = exc.args[0] if exc.args else None
            if error_code not in TRANSIENT_SQL_ERROR_CODES:
                raise
            logging.warning(f"SQL connect failed (error {error_code}, attempt {attempt + 1}) - will retry: {exc}")
    raise last_error


def replace_all(conn, rows: list[tuple], chunk_size: int = 1000) -> None:
    """DELETE everything, INSERT the fresh list - one transaction, so the table
    is never seen half-loaded."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM physiciantech.physician_facility_credentials")
    logging.info(f"Deleted {cursor.rowcount} existing row(s)")
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        placeholders = ", ".join(["(" + ", ".join(["%s"] * N_COLS) + ")"] * len(chunk))
        params = tuple(v for r in chunk for v in r)
        cursor.execute(
            "INSERT INTO physiciantech.physician_facility_credentials "
            "(ts_account_id, physician_npi, physician_name, specialty, employment_status, "
            "facility_name, health_system, division, credentialing_entity, credentialing_entity_id, "
            "cred_from, cred_to) "
            f"VALUES {placeholders}",
            params,
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logging.info("sync-physician-credentials: starting")
    token = get_domo_access_token()
    raw = fetch_credentials(token)
    logging.info(f"{len(raw)} row(s) returned from DOMO")

    rows, seen = [], set()
    skipped_key, dupes = 0, 0
    for r in raw:
        acct = _txt(r.get("TS Account ID"), 50)
        npi = _npi(r.get("NPI Number"))
        if not acct or not npi:
            skipped_key += 1
            continue
        if (acct, npi) in seen:
            dupes += 1
            continue
        seen.add((acct, npi))
        rows.append((
            acct, npi,
            _txt(r.get("Physician"), 200),
            _txt(r.get("Specialty"), 100),
            _txt(r.get("Employment Status"), 100),
            _txt(r.get("Facility"), 300),
            _txt(r.get("Health System"), 200),
            _txt(r.get("Division"), 200),
            _txt(r.get("Credentialing Entity"), 300),
            _txt(r.get("Credentialing Entity ID"), 30),
            _date(r.get("From")),
            _date(r.get("To")),
        ))

    if skipped_key:
        logging.warning(f"Skipped {skipped_key} row(s) with a blank TS Account ID or unusable NPI")
    if dupes:
        logging.info(f"Dropped {dupes} duplicate NPI x TS Account ID row(s) (first kept)")

    logging.info(f"{len(rows)} row(s) to write; "
                 f"{len({r[1] for r in rows})} physician(s), {len({r[0] for r in rows})} facility(ies)")
    logging.info(f"  specialty mix: {dict(Counter(r[3] for r in rows).most_common())}")

    if not rows:
        logging.warning("Nothing returned - leaving the table untouched rather than wiping it.")
        return

    conn = get_sql_connection()
    try:
        replace_all(conn, rows)
        logging.info(f"Wrote {len(rows)} row(s) to physiciantech.physician_facility_credentials")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

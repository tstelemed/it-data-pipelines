#requires -Version 7.0

<#
.SYNOPSIS
  Helpdesk_Ticket_Extract — SQL-only version (2026-09-01 conversion).

.NOTES
  CHANGES FROM THE DUAL-WRITE VERSION (per project owner's decisions, 9/1):

  1. CSV/Blob Storage output removed entirely. No more hdtickets/Current+Archive.
     The SQL MERGE into dbo.HelpDeskTickets — previously a best-effort side
     write wrapped in try/catch that only warned — is now the job itself. A SQL
     failure now fails the run (throws), same as every other error path here.

  2. Fixed "yesterday only" 1-day window replaced with a persisted watermark
     (TICKET_WATERMARK_TS / TICKET_WATERMARK_ID — new Automation Variables,
     same shape as Audits' existing AUDIT_WATERMARK_TS/ID). This closes the gap
     risk flagged during this conversion: the old design only ever asked for
     "yesterday," so a missed/failed run's tickets were silently never picked
     up by any later run. A watermark means a late or failed run just catches
     up on the next successful one.

     One deliberate behavior KEPT from the original: tickets created "today"
     (relative to whenever the runbook runs) are still excluded — @EndDate is
     still yesterday, same as before. This preserves the original's intent of
     not syncing an in-progress ticket before its day is over; only the START
     of the window moved from "fixed N days back" to "wherever the watermark
     left off."

  3. Ad-hoc override (-LookbackDays N > 0) now works exactly like Audits' ad-hoc
     mode: overrides the window for this run only, does NOT advance the
     persisted watermark, and DOES still write to SQL (safe — MERGE is a
     same-key upsert, so an overlapping ad-hoc run just re-upserts existing
     rows rather than duplicating them). There's no more "Archive-only, skip
     Current" distinction to preserve, since there's no CSV output left at all.

  4. Everything else — the SQL query's CTEs (ChatAgg, HdComputed, DurationCalc),
     every column expression, the HTML-cleaning XML trick, the Yes/No
     normalization, issue-classification joins, duration formatting — is
     UNCHANGED. Only the WHERE-clause boundary that decides which tickets get
     pulled changed, from a fixed calendar day to a watermark.

  FLAGGED, NOT FIXED HERE: this is a bigger structural change to the query than
  a pure "swap the storage destination" conversion (unlike Audits, which kept
  its watermark logic completely untouched). Recommend running this via the
  Test pane against a narrow ad-hoc window first and comparing row-for-row
  against a known-good CSV Archive file from the old version before trusting it
  on the daily schedule.
#>

param(
    [Parameter(Mandatory = $false)]
    [int]$LookbackDays = 0
    # 0 = normal incremental run: pulls tickets created since the persisted watermark
    #     (through yesterday, never today), writes to dbo.HelpDeskTickets, advances the
    #     watermark on success.
    # >0 = ad-hoc pull (e.g. 30, 14, or a large number for the one-time historical
    #     backfill of everything predating this conversion): overrides the watermark
    #     with "now minus N days" for THIS run only. Does NOT advance the persisted
    #     watermark. Still writes to SQL (upsert-safe).
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ================= CONFIG =================

# SOURCE: tstasks.database.windows.net / tasksdb -- dbo.Help_Desk, dbo.Chat_Logs, dbo.thd_issues
$SqlServer   = Get-AutomationVariable -Name 'SQL_SERVER'
$SqlDatabase = Get-AutomationVariable -Name 'SQL_DATABASE'
$SqlUser     = Get-AutomationVariable -Name 'SQL_USERNAME'
$SqlPassword = Get-AutomationVariable -Name 'SQL_PASSWORD'

# DESTINATION: tsitdatadev / sqldb-it-operations-dev -- dbo.HelpDeskTickets
$DestSqlServer   = Get-AutomationVariable -Name 'DEST_SQL_SERVER'
$DestSqlDatabase = Get-AutomationVariable -Name 'DEST_SQL_DATABASE'
$DestSqlUser     = Get-AutomationVariable -Name 'DEST_SQL_USERNAME'
$DestSqlPassword = Get-AutomationVariable -Name 'DEST_SQL_PASSWORD'

$DateColumns = @(
    'Created Date','Initiated Date','Assigned Time Stamp','Response Time Stamp',
    'Decision Time Stamp','Resolved Time Stamp','IT Escalation Date Time',
    'IT Tier 2 Activation Time','IT Tier 2 Response Time',
    'IT External Ticket Created Time','IT External Ticket Acknowledgment Time',
    'TC Tier 2 Activation Time','TC Tier 2 Response Time',
    'TC External Ticket Created Time','TC External Ticket Acknowledgment Time'
)

# ================= WATERMARK (new — same shape as Audits') =================
$WatermarkTsVarName = 'TICKET_WATERMARK_TS'   # string: yyyy-MM-dd HH:mm:ss.fff
$WatermarkIdVarName = 'TICKET_WATERMARK_ID'   # int (Ticket ID tie-breaker)
$RefreshDays = 14   # normal runs also re-pull tickets created in the last N days (see below)
# 2026-10-30: normal runs ALSO re-pull, whatever their age (created on/after this date):
#   * every ticket that is still not resolved in TeleCare, and
#   * every ticket resolved in the last $RefreshDays days
# so a ticket resolved weeks later (e.g. 18447) reaches dbo.HelpDeskTickets and the
# Ticketaudit resolution audit. Override with Automation Variable TICKET_REPULL_FLOOR.
$RepullFloor = '2026-09-01'
try { $v = Get-AutomationVariable -Name 'TICKET_REPULL_FLOOR'; if (-not [string]::IsNullOrWhiteSpace($v)) { $RepullFloor = $v } } catch { }

function Get-Watermark {
    $ts = $null
    $id = 0

    try { $ts = Get-AutomationVariable -Name $WatermarkTsVarName } catch { $ts = $null }
    try { $id = [int](Get-AutomationVariable -Name $WatermarkIdVarName) } catch { $id = 0 }

    # Default if never set: 30 days back, matching Audits' default.
    if ([string]::IsNullOrWhiteSpace($ts)) {
        $ts = (Get-Date).AddDays(-30).ToString('yyyy-MM-dd HH:mm:ss.fff')
        $id = 0
    }

    try {
        [void][datetime]::ParseExact($ts, 'yyyy-MM-dd HH:mm:ss.fff', [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        $ts = (Get-Date).AddDays(-30).ToString('yyyy-MM-dd HH:mm:ss.fff')
        $id = 0
    }

    [pscustomobject]@{ Ts = $ts; Id = $id }
}

function Set-Watermark {
    param(
        [Parameter(Mandatory)][string]$MaxTs,
        [Parameter(Mandatory)][int]$MaxId
    )
    Set-AutomationVariable -Name $WatermarkTsVarName -Value $MaxTs
    Set-AutomationVariable -Name $WatermarkIdVarName -Value $MaxId
}

if ($LookbackDays -gt 0) {
    $wm = [pscustomobject]@{
        Ts = (Get-Date).AddDays(-$LookbackDays).ToString('yyyy-MM-dd HH:mm:ss.fff')
        Id = 0
    }
    Write-Output "Ad-hoc pull: LookbackDays=$LookbackDays (overriding watermark for this run only; will NOT advance the persisted watermark; WILL still write to SQL)"
}
else {
    $wm = Get-Watermark

    # RE-PULL WINDOW (added 2026-10-07): a ticket used to be picked up once, the morning after it
    # was created, so anything that changed later -- most importantly the Resolved Time Stamp,
    # which Ticketaudit requires -- was never refreshed. (Confirmed: a 45-day ad-hoc re-pull cut
    # unresolved tickets older than 3 days from 130 to 31.) So on every normal run, widen the
    # query start back to $RefreshDays days ago. dbo.HelpDeskTickets is upserted by Ticket ID,
    # so re-sending rows is safe. The persisted watermark is still computed from the returned
    # rows below and only moves to the newest created ticket, exactly as before.
    $overlapStart = (Get-Date).AddDays(-$RefreshDays)
    $wmDt = [datetime]::ParseExact($wm.Ts, 'yyyy-MM-dd HH:mm:ss.fff', [System.Globalization.CultureInfo]::InvariantCulture)
    if ($wmDt -gt $overlapStart) {
        $wm = [pscustomobject]@{ Ts = $overlapStart.ToString('yyyy-MM-dd HH:mm:ss.fff'); Id = 0 }
        Write-Output "Normal run: re-pulling the last $RefreshDays days to pick up late changes (resolution, escalation)."
    }
}
Write-Output "Using watermark: TS=$($wm.Ts) ID=$($wm.Id)"
$RepullFlag = if ($LookbackDays -eq 0) { 1 } else { 0 }
if ($RepullFlag -eq 1) { Write-Output "Also re-pulling open tickets and tickets resolved in the last $RefreshDays days (created on/after $RepullFloor)." }

# ================= SQL QUERY (TICKETS ONLY) =================
# Only the boundary changed: @StartDateTime/@EndDate now derive from the watermark
# instead of a fixed N-day lookback. Everything after the WITH clauses is byte-for-byte
# identical to the dual-write version.
$SqlQuery = @"
DECLARE @WatermarkTs datetime2(3) = TRY_CONVERT(datetime2(3), '$($wm.Ts)', 121);
DECLARE @WatermarkId int = $($wm.Id);
DECLARE @EndDate DATE = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE);   -- still never includes "today"
DECLARE @RepullFloor DATE = TRY_CONVERT(date, '$RepullFloor');
DECLARE @ResolvedSince datetime2(0) = DATEADD(DAY, -$RefreshDays, GETDATE());

-- 2026-10-30: which tickets this run sends. New + recently created (watermark window) as before,
-- PLUS (normal runs only) any ticket since @RepullFloor that is still open or was resolved recently.
;WITH Picked AS (
    SELECT hd.hd_Id
    FROM dbo.Help_Desk hd
    WHERE hd.hd_Created_Date_Time < DATEADD(DAY, 1, @EndDate)
      AND (
            hd.hd_Created_Date_Time > @WatermarkTs
            OR (hd.hd_Created_Date_Time = @WatermarkTs AND hd.hd_Id > @WatermarkId)
            OR ($RepullFlag = 1 AND hd.hd_Created_Date_Time >= @RepullFloor
                AND (ISNULL(hd.hd_status, '') <> 'resolved' OR hd.hd_resolved_date_time >= @ResolvedSince))
          )
),
-- 2026-10-30: the WHOLE chat for each picked ticket (was: only lines since the watermark date, which
-- cut off older lines whenever a ticket was re-pulled and overwrote the full chat with a partial one).
ChatAgg AS (
    SELECT
        cl.cl_hd_Id,
        STRING_AGG(CAST(cl.cl_Message AS NVARCHAR(MAX)), ' ') WITHIN GROUP (ORDER BY cl.cl_Date_Time ASC) AS [Chat Messages]
    FROM dbo.Chat_Logs cl
    JOIN Picked p ON p.hd_Id = cl.cl_hd_Id
    GROUP BY cl.cl_hd_Id
),
HdComputed AS (
    SELECT
        hd.*,
        CASE
            WHEN hd.hd_decision_date_time IS NULL AND hd.hd_status = 'resolved'
                THEN hd.hd_resolved_date_time
            ELSE hd.hd_decision_date_time
        END AS DecisionDT,
        (
            SELECT STRING_AGG(JSON_VALUE(value, '$.description'), ' ')
            FROM OPENJSON(hd.hd_Issue_Description)
        ) AS ActivityNote
    FROM dbo.Help_Desk hd
    JOIN Picked p ON p.hd_Id = hd.hd_Id
),
DurationCalc AS (
    SELECT
        hd.*,
        DATEDIFF(SECOND, hd.hd_ticket_initiated_time, hd.hd_ticket_assigned_time) AS AssignmentSeconds,
        DATEDIFF(SECOND, hd.hd_ticket_initiated_time, hd.hd_response_time)       AS ResponseSeconds,
        DATEDIFF(SECOND, hd.hd_ticket_initiated_time, hd.DecisionDT)             AS DecisionSeconds
    FROM HdComputed hd
)
SELECT
    hd.hd_Id AS [Ticket ID],
    hd.hd_case_Number AS [Case Number],
    hd.hd_case_call_back_number AS [Call Back Number],
    FORMAT(hd.hd_Created_Date_Time, 'yyyy-MM-dd HH:mm:ss') AS [Created Date],
    FORMAT(hd.hd_ticket_initiated_time, 'yyyy-MM-dd HH:mm:ss') AS [Initiated Date],
    FORMAT(hd.hd_ticket_assigned_time, 'yyyy-MM-dd HH:mm:ss') AS [Assigned Time Stamp],
    FORMAT(hd.hd_response_time, 'yyyy-MM-dd HH:mm:ss') AS [Response Time Stamp],
    FORMAT(hd.DecisionDT, 'yyyy-MM-dd HH:mm:ss') AS [Decision Time Stamp],
    FORMAT(hd.hd_resolved_date_time, 'yyyy-MM-dd HH:mm:ss') AS [Resolved Time Stamp],
    hd.hd_Assigned_to AS [Technician],
    hd.hd_cart_name AS [Cart Name],
    hd.hd_facility_name AS [Facility Name],
    hd.hd_video_software AS [Video Software],
    hd.hd_case_type_info AS [Case Type],
    hd.hd_status AS [Ticket Status],
    hd.hd_task_physician_name AS [Physician Name],
    ca.[Chat Messages],
    hd.hd_description AS [Description],
    hd.hd_Decision AS [Decision],
    hd.hd_Resolution AS [Resolution],
    hd.hd_addendum AS [Addendum],
    hd.hd_explanation AS [Explanation > 5:00],

    REPLACE(
        TRY_CAST(
            '<x>' + REPLACE(
                REPLACE(CAST(hd.hd_Summary AS NVARCHAR(MAX)), '&', '&amp;'),
                '<br>', '<br/>'
            ) + '</x>' AS XML
        ).value('.', 'nvarchar(max)'),
        NCHAR(8217), ''''
    ) AS [Summary],

    REPLACE(
        TRY_CAST(
            '<x>' + REPLACE(
                REPLACE(CAST(hd.hd_problem_summary AS NVARCHAR(MAX)), '&', '&amp;'),
                '<br>', '<br/>'
            ) + '</x>' AS XML
        ).value('.', 'nvarchar(max)'),
        NCHAR(8217), ''''
    ) AS [Problem Summary],

    REPLACE(
        TRY_CAST(
            '<x>' + REPLACE(
                REPLACE(CAST(hd.hd_investigation AS NVARCHAR(MAX)), '&', '&amp;'),
                '<br>', '<br/>'
            ) + '</x>' AS XML
        ).value('.', 'nvarchar(max)'),
        NCHAR(8217), ''''
    ) AS [Investigation],

    REPLACE(
        TRY_CAST(
            '<x>' + REPLACE(
                REPLACE(CAST(hd.hd_troubleshooting AS NVARCHAR(MAX)), '&', '&amp;'),
                '<br>', '<br/>'
            ) + '</x>' AS XML
        ).value('.', 'nvarchar(max)'),
        NCHAR(8217), ''''
    ) AS [Troubleshooting],

    CASE
        WHEN TRY_CONVERT(int, hd.hd_pass_to_physician) = 1 THEN 'Yes'
        WHEN UPPER(LTRIM(RTRIM(CAST(hd.hd_pass_to_physician AS NVARCHAR(50)))))
             IN ('YES','Y','TRUE','T','1') THEN 'Yes'
        ELSE 'No'
    END AS [Pass To Physician],

    CASE
        WHEN TRY_CONVERT(int, hd.hd_audio_call) = 1 THEN 'Yes'
        WHEN UPPER(LTRIM(RTRIM(CAST(hd.hd_audio_call AS NVARCHAR(50)))))
             IN ('YES','Y','TRUE','T','1','AUDIO','AUDIOONLY') THEN 'Yes'
        ELSE 'No'
    END AS [Audio Call],

    CASE
        WHEN TRY_CONVERT(int, hd.hd_first_call_resolution) = 1 THEN 'Yes'
        WHEN UPPER(LTRIM(RTRIM(CAST(hd.hd_first_call_resolution AS NVARCHAR(50)))))
             IN ('YES','Y','TRUE','T','1','FCR') THEN 'Yes'
        ELSE 'No'
    END AS [FCR],

    CASE
        WHEN TRY_CONVERT(int, hd.hd_keep_with_physician) = 1 THEN 'Yes'
        WHEN UPPER(LTRIM(RTRIM(CAST(hd.hd_keep_with_physician AS NVARCHAR(50)))))
             IN ('YES','Y','TRUE','T','1','PHYSICIAN') THEN 'Yes'
        ELSE 'No'
    END AS [Keep With Physician],

    hd.hd_facility_issue AS [Facility Issue Id],
    ti_fac.IssueName     AS [Facility Issue Name],
    ti_fac.IssueCategory AS [Facility Issue Category],

    hd.hd_physician_issue AS [Physician Issue Id],
    ti_phys.IssueName     AS [Physician Issue Name],
    ti_phys.IssueCategory AS [Physician Issue Category],

    CASE
        WHEN hd.AssignmentSeconds IS NULL THEN NULL
        ELSE CAST(hd.AssignmentSeconds / 3600 AS VARCHAR) + 'h ' +
             CAST((hd.AssignmentSeconds % 3600) / 60 AS VARCHAR) + 'm ' +
             CAST(hd.AssignmentSeconds % 60 AS VARCHAR) + 's'
    END AS [Assignment Duration],

    CASE
        WHEN hd.ResponseSeconds IS NULL THEN NULL
        ELSE CAST(hd.ResponseSeconds / 3600 AS VARCHAR) + 'h ' +
             CAST((hd.ResponseSeconds % 3600) / 60 AS VARCHAR) + 'm ' +
             CAST(hd.ResponseSeconds % 60 AS VARCHAR) + 's'
    END AS [Response Duration],

    CASE
        WHEN hd.DecisionSeconds IS NULL THEN NULL
        ELSE CAST(hd.DecisionSeconds / 3600 AS VARCHAR) + 'h ' +
             CAST((hd.DecisionSeconds % 3600) / 60 AS VARCHAR) + 'm ' +
             CAST(hd.DecisionSeconds % 60 AS VARCHAR) + 's'
    END AS [Decision Duration],

    hd.hd_itescalation AS [IT Escalation],
    FORMAT(TRY_CONVERT(datetime2, hd.hd_itescalation_date_time), 'yyyy-MM-dd HH:mm:ss') AS [IT Escalation Date Time],
    hd.hd_tier1_escalation AS [IT Tier 1 Escalation],
    hd.hd_tier2_escalation AS [IT Tier 2 Escalation],
    FORMAT(TRY_CONVERT(datetime2, hd.hd_tier2_activation_time), 'yyyy-MM-dd HH:mm:ss') AS [IT Tier 2 Activation Time],
    FORMAT(TRY_CONVERT(datetime2, hd.hd_tier2_response_time), 'yyyy-MM-dd HH:mm:ss') AS [IT Tier 2 Response Time],
    hd.hd_tier2_reason AS [IT Tier 2 Reason],
    FORMAT(TRY_CONVERT(datetime2, hd.hd_ops_external_ticket_created_time), 'yyyy-MM-dd HH:mm:ss') AS [IT External Ticket Created Time],
    FORMAT(TRY_CONVERT(datetime2, hd.hd_ops_external_ticket_acknowledgment_time), 'yyyy-MM-dd HH:mm:ss') AS [IT External Ticket Acknowledgment Time],
    hd.hd_ops_acknowledged_by AS [IT External Acknowledged By],
    hd.hd_ops_issue_id AS [IT External Issue Id],
    hd.hd_ops_status AS [IT External Status],

    FORMAT(TRY_CONVERT(datetime2, hd.tc_tier2_activation_time), 'yyyy-MM-dd HH:mm:ss') AS [TC Tier 2 Activation Time],
    FORMAT(TRY_CONVERT(datetime2, hd.tc_tier2_response_time), 'yyyy-MM-dd HH:mm:ss') AS [TC Tier 2 Response Time],
    hd.tc_tier2_reason AS [TC Tier 2 Reason],
    FORMAT(TRY_CONVERT(datetime2, hd.tc_ops_external_ticket_created_time), 'yyyy-MM-dd HH:mm:ss') AS [TC External Ticket Created Time],
    FORMAT(TRY_CONVERT(datetime2, hd.tc_ops_external_ticket_acknowledgment_time), 'yyyy-MM-dd HH:mm:ss') AS [TC External Ticket Acknowledgment Time],
    hd.tc_ops_acknowledged_by AS [TC External Acknowledged By],
    hd.tc_ops_issue_id AS [TC External Issue Id],
    hd.tc_ops_status AS [TC External Status],

    hd.hd_created_by_task_in_telecare AS [Created By Task In Telecare]

FROM DurationCalc hd
LEFT JOIN ChatAgg ca
    ON ca.cl_hd_Id = hd.hd_Id

LEFT JOIN dbo.thd_issues AS ti_fac
    ON ti_fac.Id = TRY_CONVERT(int, hd.hd_facility_issue)

LEFT JOIN dbo.thd_issues AS ti_phys
    ON ti_phys.Id = TRY_CONVERT(int, hd.hd_physician_issue)

ORDER BY hd.hd_Created_Date_Time ASC, hd.hd_Id ASC;
"@

# ================= SQL DESTINATION HELPER (unchanged from the dual-write version) =================
function Push-TicketsToSql {
    param(
        [Parameter(Mandatory)][System.Data.DataTable]$Table,
        [Parameter(Mandatory)][string]$DestServer,
        [Parameter(Mandatory)][string]$DestDatabase,
        [Parameter(Mandatory)][string]$DestUser,
        [Parameter(Mandatory)][string]$DestPassword,
        [Parameter(Mandatory)][string[]]$DateColumns
    )

    $dataColumns = foreach ($c in $Table.Columns) { $c.ColumnName }

    function ParamName([string]$col) { '@p_' + ($col -replace '[^a-zA-Z0-9]', '_') }

    $mergeSql = @"
MERGE dbo.HelpDeskTickets AS target
USING (SELECT $(($dataColumns | ForEach-Object { "$(ParamName $_) AS [$_]" }) -join ', ')) AS src
ON target.[Ticket ID] = src.[Ticket ID]
WHEN MATCHED THEN UPDATE SET
    $(($dataColumns | Where-Object { $_ -ne 'Ticket ID' } | ForEach-Object { "target.[$_] = src.[$_]" }) -join ",`n    "),
    target.updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
        ($(($dataColumns | ForEach-Object { "[$_]" }) -join ', '), retrieved_at, updated_at)
        VALUES ($(($dataColumns | ForEach-Object { "src.[$_]" }) -join ', '), SYSUTCDATETIME(), SYSUTCDATETIME());
"@

    $connString = "Server=tcp:$DestServer,1433;Database=$DestDatabase;User ID=$DestUser;Password=$DestPassword;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    $conn = New-Object System.Data.SqlClient.SqlConnection $connString
    $conn.Open()

    try {
        $upserted = 0
        foreach ($row in $Table.Rows) {
            $cmd = $conn.CreateCommand()
            $cmd.CommandTimeout = 60
            $cmd.CommandText = $mergeSql

            foreach ($col in $dataColumns) {
                $raw = $row[$col]
                $value = $null

                if ($null -eq $raw -or $raw -is [DBNull] -or [string]::IsNullOrWhiteSpace([string]$raw)) {
                    $value = [DBNull]::Value
                }
                elseif ($col -eq 'Ticket ID') {
                    $value = [int]$raw
                }
                elseif ($col -in $DateColumns) {
                    try {
                        $value = [datetime]::ParseExact([string]$raw, 'yyyy-MM-dd HH:mm:ss', [System.Globalization.CultureInfo]::InvariantCulture)
                    }
                    catch {
                        $value = [DBNull]::Value
                    }
                }
                else {
                    $value = [string]$raw
                }

                [void]$cmd.Parameters.AddWithValue((ParamName $col), $value)
            }

            [void]$cmd.ExecuteNonQuery()
            $upserted++
        }
        return $upserted
    }
    finally {
        $conn.Close()
    }
}

# ================= MAIN =================

if ([string]::IsNullOrWhiteSpace($SqlServer))          { throw "Missing Automation Variable: SQL_SERVER" }
if ([string]::IsNullOrWhiteSpace($SqlDatabase))        { throw "Missing Automation Variable: SQL_DATABASE" }
if ([string]::IsNullOrWhiteSpace($SqlUser))            { throw "Missing Automation Variable: SQL_USERNAME" }
if ([string]::IsNullOrWhiteSpace($SqlPassword))        { throw "Missing Automation Variable: SQL_PASSWORD" }
if ([string]::IsNullOrWhiteSpace($DestSqlServer))      { throw "Missing Automation Variable: DEST_SQL_SERVER" }
if ([string]::IsNullOrWhiteSpace($DestSqlDatabase))    { throw "Missing Automation Variable: DEST_SQL_DATABASE" }
if ([string]::IsNullOrWhiteSpace($DestSqlUser))        { throw "Missing Automation Variable: DEST_SQL_USERNAME" }
if ([string]::IsNullOrWhiteSpace($DestSqlPassword))    { throw "Missing Automation Variable: DEST_SQL_PASSWORD" }
if ($LookbackDays -lt 0)                               { throw "LookbackDays cannot be negative." }

try {
    Write-Output "Connecting to Azure SQL (source)..."
    $connString = "Server=tcp:$SqlServer,1433;Database=$SqlDatabase;User ID=$SqlUser;Password=$SqlPassword;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    $conn = New-Object System.Data.SqlClient.SqlConnection $connString
    $conn.Open()

    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 300
    $cmd.CommandText = $SqlQuery

    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
    $table   = New-Object System.Data.DataTable
    [void]$adapter.Fill($table)

    $conn.Close()

    Write-Output "Rows returned: $($table.Rows.Count)"
    if ($table.Rows.Count -eq 0) {
        Write-Warning "No NEW tickets since watermark. Exiting without writing."
        return
    }

    # Compute max watermark (Created Date + Ticket ID tie-breaker).
    $maxDT = [datetime]::MinValue
    $maxId = 0

    foreach ($r in $table.Rows) {
        $id = [int]$r['Ticket ID']
        $ts = [string]$r['Created Date']
        $dt = [datetime]::ParseExact($ts, 'yyyy-MM-dd HH:mm:ss', [System.Globalization.CultureInfo]::InvariantCulture)

        if ($dt -gt $maxDT) {
            $maxDT = $dt
            $maxId = $id
        } elseif ($dt -eq $maxDT -and $id -gt $maxId) {
            $maxId = $id
        }
    }

    # SQL write — this IS the job now (no CSV safety net). A failure here fails the run.
    Write-Output "Writing to dbo.HelpDeskTickets on $DestSqlServer/$DestSqlDatabase..."
    $upserted = Push-TicketsToSql -Table $table -DestServer $DestSqlServer -DestDatabase $DestSqlDatabase -DestUser $DestSqlUser -DestPassword $DestSqlPassword -DateColumns $DateColumns
    Write-Output "SQL upsert complete: $upserted row(s)."

    if ($LookbackDays -eq 0) {
        # Watermark stores yyyy-MM-dd HH:mm:ss.fff for consistency with Audits' format
        # (source data here has no milliseconds, so .000 is fine).
        $newTs = $maxDT.ToString('yyyy-MM-dd HH:mm:ss.fff')
        Set-Watermark -MaxTs $newTs -MaxId $maxId
        Write-Output "Watermark updated: TS=$newTs ID=$maxId"
    }
    else {
        Write-Output "Ad-hoc pull - watermark left unchanged."
    }

    Write-Output "Done."
}
catch {
    Write-Error $_
    throw
}
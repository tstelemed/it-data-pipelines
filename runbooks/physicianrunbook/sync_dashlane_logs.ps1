<#
Azure Automation Runbook: sync_dashlane_logs
Runs on: Hybrid Worker group (Windows Server VM with dcli + OpenSSL installed)
Runtime: PowerShell 5.1

What it does
  Pulls Dashlane audit logs with the Dashlane CLI (dcli t logs), one day at a time,
  keeps credential events on hospital / telehealth sites listed in
  physiciantech.dashlane_site_map, and writes them to physiciantech.dashlane_events.
  Sites not in the map are only counted per day (physiciantech.dashlane_unmapped_daily).
  Cards, IDs, secure notes, devices and IP details are never stored.

First run (no watermark yet)
  Password created/modified/deleted events from 2026-01-12 (when Dashlane started
  logging them) + autofill/copy/reveal events for the last 120 days.
Later runs
  Re-pulls from the day before the watermark through today; duplicates are skipped.

Automation variables
  DASHLANE_CLI_KEY            (encrypted) CLI key from Dashlane Admin Console > Developer Access
  PHYSICIANTECH_SQL_PASSWORD  (encrypted) existing physiciantech_pipeline password
#>

$ErrorActionPreference = 'Stop'

# ---------------- settings ----------------
$SqlServer      = 'tsitdatadev.database.windows.net'
$SqlDatabase    = 'sqldb-it-operations-dev'
$SqlUser        = 'physiciantech_pipeline'
$JobName        = 'dashlane_logs'
$DcliPath       = 'C:\Tools\Dashlane\dcli.exe'
$OpenSslDir     = 'C:\Program Files\OpenSSL-Win64\bin'
$ChangesFrom    = [datetime]'2026-01-12'   # first day Dashlane logged password events
$UsageDays      = 120                      # history kept for autofill/copy/reveal on first run

$ChangeTypes = @('user_created_credential', 'user_modified_credential', 'user_deleted_credential')
$UsageTypes  = @('user_performed_autofill_credential', 'user_copied_credential_field',
                 'user_revealed_credential_field', 'user_pasted_credential_different_website')

# ---------------- helpers ----------------
function Get-BaseDomain([string]$value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return '' }
    $h = $value.Trim().ToLowerInvariant() -replace '^[a-z]+://', ''
    $h = ($h -split '[/:]')[0]
    $parts = $h.Split('.') | Where-Object { $_ -ne '' }
    if ($parts.Count -ge 2) { return ($parts[-2] + '.' + $parts[-1]) }
    return $h
}

function New-SqlConnection {
    $pw = Get-AutomationVariable -Name 'PHYSICIANTECH_SQL_PASSWORD'
    $cs = "Server=tcp:$SqlServer,1433;Database=$SqlDatabase;User ID=$SqlUser;Password=$pw;Encrypt=True;TrustServerCertificate=False;Connection Timeout=60;"
    $c = New-Object System.Data.SqlClient.SqlConnection $cs
    $c.Open()
    return $c
}

function Invoke-Sql($conn, [string]$sql, [hashtable]$params = @{}) {
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $cmd.CommandTimeout = 300
    foreach ($k in $params.Keys) { [void]$cmd.Parameters.AddWithValue($k, $params[$k]) }
    return $cmd.ExecuteNonQuery()
}

function Get-SqlScalar($conn, [string]$sql) {
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    return $cmd.ExecuteScalar()
}

function Invoke-DcliDay([datetime]$day) {
    $startMs = ([DateTimeOffset]::new($day.Date, [TimeSpan]::Zero)).ToUnixTimeMilliseconds()
    $endMs   = ([DateTimeOffset]::new($day.Date.AddDays(1), [TimeSpan]::Zero)).ToUnixTimeMilliseconds()
    $out = & $DcliPath t logs --start $startMs --end $endMs 2>&1
    $errors = @($out | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] })
    if ($errors.Count -gt 0 -or $LASTEXITCODE -ne 0) {
        throw "dcli failed for $($day.ToString('yyyy-MM-dd')): $($errors -join ' | ')"
    }
    return @($out | Where-Object { $_ -is [string] -and $_.Trim() -ne '' })
}

# ---------------- setup ----------------
if (-not (Test-Path $DcliPath)) { throw "dcli not found at $DcliPath" }
if (-not (Test-Path (Join-Path $OpenSslDir 'openssl.exe'))) { throw "OpenSSL not found in $OpenSslDir" }
$env:PATH = "$OpenSslDir;$env:PATH"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$conn = New-SqlConnection
try {
    $env:DASHLANE_ENROLLED_TEAM_DEVICE_KEYS = Get-AutomationVariable -Name 'DASHLANE_CLI_KEY'

    # Site map
    $siteMap = @{}
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = 'SELECT domain FROM physiciantech.dashlane_site_map'
    $rdr = $cmd.ExecuteReader()
    while ($rdr.Read()) { $siteMap[$rdr.GetString(0).ToLowerInvariant()] = $true }
    $rdr.Close()
    Write-Output "Site map: $($siteMap.Count) domains"

    # Window
    $today = [datetime]::UtcNow.Date
    $wm = Get-SqlScalar $conn "SELECT last_period_end FROM physiciantech.etl_watermark WHERE job_name = '$JobName'"
    if ($wm -eq $null -or $wm -is [System.DBNull]) {
        $firstDay   = $ChangesFrom.Date
        $usageStart = $today.AddDays(-$UsageDays)
        Write-Output "No watermark - first run: changes from $($firstDay.ToString('yyyy-MM-dd')), usage from $($usageStart.ToString('yyyy-MM-dd'))"
    } else {
        $firstDay   = ([datetime]$wm).Date.AddDays(-1)
        $usageStart = $firstDay
        Write-Output "Watermark $(([datetime]$wm).ToString('yyyy-MM-dd')) - pulling from $($firstDay.ToString('yyyy-MM-dd'))"
    }

    # Staging table for bulk insert
    Invoke-Sql $conn @"
CREATE TABLE #stage (
    uuid varchar(36) NOT NULL, event_utc datetime2(3) NOT NULL, log_type varchar(60) NOT NULL,
    author_login varchar(200) NOT NULL, domain varchar(200) NOT NULL, site_host varchar(255) NULL,
    credential_login nvarchar(255) NULL, field varchar(50) NULL)
"@ | Out-Null

    $epoch = [DateTimeOffset]::new(1970,1,1,0,0,0,[TimeSpan]::Zero)
    $totalPulled = 0; $totalKept = 0; $totalInserted = 0

    for ($day = $firstDay; $day -le $today; $day = $day.AddDays(1)) {
        $dayEnd = $day.AddDays(1)
        $keepUsage = ($day -ge $usageStart)

        $table = New-Object System.Data.DataTable
        foreach ($col in 'uuid','event_utc','log_type','author_login','domain','site_host','credential_login','field') {
            if ($col -eq 'event_utc') { [void]$table.Columns.Add($col, [datetime]) } else { [void]$table.Columns.Add($col, [string]) }
        }
        $unmapped = @{}

        $lines = Invoke-DcliDay $day
        $totalPulled += $lines.Count

        foreach ($line in $lines) {
            $ev = $line | ConvertFrom-Json
            $type = [string]$ev.log_type
            $isChange = $ChangeTypes -contains $type
            $isUsage  = $UsageTypes -contains $type
            if (-not ($isChange -or ($isUsage -and $keepUsage))) { continue }

            $evUtc = $epoch.AddMilliseconds([double]$ev.date_time).UtcDateTime
            if ($evUtc -lt $day -or $evUtc -ge $dayEnd) { continue }   # keep only this day

            $p = $ev.properties
            if ($isChange) { $siteHost = [string]$p.domain_url } else { $siteHost = [string]$p.credential_domain }
            if ($type -eq 'user_performed_autofill_credential' -and $p.autofilled_domain) { $hostFull = [string]$p.autofilled_domain } else { $hostFull = $siteHost }
            $base = Get-BaseDomain $siteHost

            if (-not $siteMap.ContainsKey($base)) {
                $key = $(if ($base -eq '') { '(blank)' } else { $base })
                if ($unmapped.ContainsKey($key)) { $unmapped[$key]++ } else { $unmapped[$key] = 1 }
                continue
            }

            $r = $table.NewRow()
            $r.uuid             = [string]$ev.uuid
            $r.event_utc        = $evUtc
            $r.log_type         = $type
            $r.author_login     = ([string]$p.author_login).ToLowerInvariant()
            $r.domain           = $base
            $r.site_host        = $(if ($hostFull) { $hostFull.ToLowerInvariant() } else { [DBNull]::Value })
            $r.credential_login = $(if ($p.credential_login) { [string]$p.credential_login } else { [DBNull]::Value })
            $r.field            = $(if ($p.field) { [string]$p.field } else { [DBNull]::Value })
            $table.Rows.Add($r)
        }

        $inserted = 0
        if ($table.Rows.Count -gt 0) {
            Invoke-Sql $conn 'TRUNCATE TABLE #stage' | Out-Null
            $bulk = New-Object System.Data.SqlClient.SqlBulkCopy($conn)
            $bulk.DestinationTableName = '#stage'
            $bulk.BulkCopyTimeout = 300
            foreach ($col in $table.Columns) { [void]$bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) }
            $bulk.WriteToServer($table)
            $inserted = Invoke-Sql $conn @"
INSERT INTO physiciantech.dashlane_events (uuid, event_utc, log_type, author_login, domain, site_host, credential_login, field)
SELECT DISTINCT s.uuid, s.event_utc, s.log_type, s.author_login, s.domain, s.site_host, s.credential_login, s.field
FROM #stage s
WHERE NOT EXISTS (SELECT 1 FROM physiciantech.dashlane_events e WHERE e.uuid = s.uuid)
"@
        }

        foreach ($k in $unmapped.Keys) {
            Invoke-Sql $conn @"
MERGE physiciantech.dashlane_unmapped_daily AS t
USING (SELECT @d AS domain, @dt AS event_date, @n AS events) AS s
ON t.domain = s.domain AND t.event_date = s.event_date
WHEN MATCHED THEN UPDATE SET events = s.events
WHEN NOT MATCHED THEN INSERT (domain, event_date, events) VALUES (s.domain, s.event_date, s.events);
"@ @{ '@d' = $k; '@dt' = $day; '@n' = $unmapped[$k] } | Out-Null
        }

        $totalKept += $table.Rows.Count
        $totalInserted += $inserted
        Write-Output ("{0}: pulled {1}, kept {2}, new {3}, unmapped sites {4}" -f $day.ToString('yyyy-MM-dd'), $lines.Count, $table.Rows.Count, $inserted, $unmapped.Count)
    }

    # Watermark = today (next run starts from yesterday)
    Invoke-Sql $conn @"
MERGE physiciantech.etl_watermark AS t
USING (SELECT @j AS job_name) AS s ON t.job_name = s.job_name
WHEN MATCHED THEN UPDATE SET last_period_end = @p, last_run_utc = sysutcdatetime(), rows_loaded = @r
WHEN NOT MATCHED THEN INSERT (job_name, last_period_end, last_run_utc, rows_loaded) VALUES (@j, @p, sysutcdatetime(), @r);
"@ @{ '@j' = $JobName; '@p' = $today; '@r' = $totalInserted } | Out-Null

    Write-Output "Done. Pulled $totalPulled events, kept $totalKept, inserted $totalInserted new."
}
finally {
    Remove-Item Env:DASHLANE_ENROLLED_TEAM_DEVICE_KEYS -ErrorAction SilentlyContinue
    if ($conn) { $conn.Close() }
}
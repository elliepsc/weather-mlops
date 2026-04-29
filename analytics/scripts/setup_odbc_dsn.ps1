# setup_odbc_dsn.ps1
#
# Registers a System DSN "WeatherMLOps_DuckDB" pointing to analytics.duckdb.
# Run once (as Administrator) after installing the DuckDB ODBC driver.
#
# DuckDB ODBC driver download:
#   https://github.com/duckdb/duckdb/releases/latest
#   File: duckdb_odbc-windows-amd64.zip  → extract → run "odbc_install.exe"
#
# Then run this script (PowerShell as Admin):
#   powershell -ExecutionPolicy Bypass -File analytics\scripts\setup_odbc_dsn.ps1

param(
    [string]$DsnName   = "WeatherMLOps_DuckDB",
    [string]$DbRelPath = "data\analytics.duckdb"
)

# Resolve absolute path from repo root (script lives in analytics/scripts/)
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\")).Path
$DbPath   = Join-Path $RepoRoot $DbRelPath

if (-not (Test-Path $DbPath)) {
    Write-Error "DuckDB file not found: $DbPath"
    Write-Error "Run first: make analytics-run"
    exit 1
}

$OdbcKey = "HKLM:\SOFTWARE\ODBC\ODBC.INI\$DsnName"
$OdbcList = "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"

# Check if DuckDB driver is installed
$DriverKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DuckDB Driver"
if (-not (Test-Path $DriverKey)) {
    Write-Error "DuckDB ODBC driver not found in registry."
    Write-Error "Download and install from: https://github.com/duckdb/duckdb/releases/latest"
    Write-Error "File: duckdb_odbc-windows-amd64.zip -> odbc_install.exe"
    exit 1
}

# Create or update DSN
if (-not (Test-Path $OdbcKey)) {
    New-Item -Path $OdbcKey -Force | Out-Null
}
Set-ItemProperty -Path $OdbcKey -Name "Driver"   -Value (Get-ItemPropertyValue $DriverKey -Name "Driver")
Set-ItemProperty -Path $OdbcKey -Name "Database" -Value $DbPath
Set-ItemProperty -Path $OdbcKey -Name "DSN"      -Value $DsnName

# Register in ODBC Data Sources list
if (-not (Test-Path $OdbcList)) {
    New-Item -Path $OdbcList -Force | Out-Null
}
Set-ItemProperty -Path $OdbcList -Name $DsnName -Value "DuckDB Driver"

Write-Host ""
Write-Host "DSN created: $DsnName"
Write-Host "Database   : $DbPath"
Write-Host ""
Write-Host "Next steps in Power BI Desktop:"
Write-Host "  1. Get Data -> ODBC"
Write-Host "  2. Select DSN: $DsnName"
Write-Host "  3. Mode: Import (DirectQuery not supported for local DuckDB)"
Write-Host "  4. Tables are under schemas: main_marts, main_intermediate, main_core"
Write-Host ""
Write-Host "To refresh data after a new dbt run:"
Write-Host "  Power BI Home -> Refresh  (or schedule via Power BI Service)"

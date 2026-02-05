param(
  [string]$ProjectRoot = "",
  [string]$TaskName = "CompanyLocWeeklyIngest",
  [string]$WeekDay = "SUN",
  [string]$Time = "03:00",
  [string]$Companies = "nvidia,nokia"
)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
  $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$runner = Join-Path $ProjectRoot "scripts\run_weekly_ingest.ps1"
if (!(Test-Path $runner)) {
  Write-Error "runner not found: $runner"
  exit 2
}

$tr = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`" -ProjectRoot `"$ProjectRoot`" -Companies `"$Companies`""
schtasks /Create /F /SC WEEKLY /D $WeekDay /ST $Time /TN $TaskName /TR $tr | Out-Host

Write-Host "Task registered: $TaskName ($WeekDay $Time) companies=$Companies"

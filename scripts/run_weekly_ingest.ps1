param(
  [string]$ProjectRoot = "",
  [string]$Companies = "nvidia,nokia"
)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
  $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $ProjectRoot

$python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (Test-Path $python) {
  & $python -u -m backend.py.pipeline.ingest_weekly --companies $Companies
  exit $LASTEXITCODE
}

python -u -m backend.py.pipeline.ingest_weekly --companies $Companies
exit $LASTEXITCODE

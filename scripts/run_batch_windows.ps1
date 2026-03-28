param(
    [int]$Year = 2000,
    [switch]$RetryIncomplete,
    [string]$RetryStatuses = "",
    [switch]$SanitizeOnly
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$FallbackPython = "py"
$ScriptPath = Join-Path $ProjectRoot "scripts\income_scraper.py"
$LogPath = Join-Path $ProjectRoot ("output\windows_batch_{0}.log" -f $Year)

if (Test-Path $VenvPython) {
    $PythonExe = $VenvPython
    $BaseArgs = @($ScriptPath)
} else {
    $PythonExe = $FallbackPython
    $BaseArgs = @("-3", $ScriptPath)
}

$ArgList = @()
$ArgList += $BaseArgs
$ArgList += @("--year", $Year.ToString())

if ($SanitizeOnly) {
    $ArgList += "--sanitize-only"
} else {
    $ArgList += "--headless"
    if ($RetryIncomplete) {
        $ArgList += "--retry-incomplete"
    }
    if ($RetryStatuses.Trim()) {
        $ArgList += @("--retry-statuses", $RetryStatuses.Trim())
    }
}

$env:CNKI_USE_LOCAL_CHROME_PROFILE = "1"
$env:CNKI_CHROME_PROFILE_MODE = "clone"

Write-Host "ProjectRoot: $ProjectRoot"
Write-Host "PythonExe  : $PythonExe"
Write-Host "ScriptPath : $ScriptPath"
Write-Host "LogPath    : $LogPath"
Write-Host "Args       : $($ArgList -join ' ')"

New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "output") | Out-Null

& $PythonExe @ArgList *>> $LogPath
$ExitCode = $LASTEXITCODE
Write-Host "ExitCode   : $ExitCode"
exit $ExitCode

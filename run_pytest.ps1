$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$userSite = Join-Path $env:APPDATA "Python\Python314\site-packages"
$vendorDir = Join-Path $projectRoot ".vendor"
$cacheDir = Join-Path $projectRoot "tests\.tmp\pytest_cache"

$pythonPathEntries = @()
if (Test-Path $userSite) {
    $pythonPathEntries += $userSite
}
if (Test-Path $vendorDir) {
    $pythonPathEntries += $vendorDir
}
if ($env:PYTHONPATH) {
    $pythonPathEntries += $env:PYTHONPATH
}

$env:PYTHONPATH = ($pythonPathEntries | Select-Object -Unique) -join ";"

New-Item -ItemType Directory -Path $cacheDir -Force | Out-Null

python -m pytest -o cache_dir="$cacheDir" @args

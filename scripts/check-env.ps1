<#
Checks the local toolchain before working on this project:
  - Go 1.21+ on PATH
  - gcc (needed only for `go test -race`, which requires cgo's C toolchain on Windows)
Run from anywhere: .\scripts\check-env.ps1
#>

$ErrorActionPreference = "Continue"

function Test-Command($name) {
    $null = Get-Command $name -ErrorAction SilentlyContinue
    return $?
}

Write-Host "== Go ==" -ForegroundColor Cyan
if (Test-Command "go") {
    $goVersionOutput = go version
    Write-Host $goVersionOutput

    if ($goVersionOutput -match "go(\d+)\.(\d+)") {
        $major = [int]$Matches[1]
        $minor = [int]$Matches[2]
        if ($major -gt 1 -or ($major -eq 1 -and $minor -ge 21)) {
            Write-Host "OK: Go $major.$minor >= 1.21" -ForegroundColor Green
        } else {
            Write-Host "WARN: Go $major.$minor is older than 1.21. Upgrade with:" -ForegroundColor Yellow
            Write-Host "  winget install --id GoLang.Go -e"
        }
    }
} else {
    Write-Host "MISSING: 'go' not found on PATH." -ForegroundColor Red
    Write-Host "Install with:"
    Write-Host "  winget install --id GoLang.Go -e"
    Write-Host "Then restart your terminal so PATH picks it up."
}

Write-Host ""
Write-Host "== gcc (only required for 'go test -race') ==" -ForegroundColor Cyan
if (Test-Command "gcc") {
    gcc --version | Select-Object -First 1
    Write-Host "OK: gcc found." -ForegroundColor Green
} else {
    Write-Host "MISSING: 'gcc' not found on PATH." -ForegroundColor Yellow
    Write-Host "Not required for normal build/test, only for '-race'. Install with:"
    Write-Host "  winget install --id BrechtSanders.WinLibs.POSIX.UCRT -e"
    Write-Host "Then restart your terminal and re-run this script."
}

Write-Host ""
Write-Host "== git ==" -ForegroundColor Cyan
if (Test-Command "git") {
    git --version
} else {
    Write-Host "MISSING: 'git' not found on PATH." -ForegroundColor Red
}

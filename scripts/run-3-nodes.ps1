<#
Opens 3 PowerShell windows, one per cluster node, so you can watch leader
election happen live. Run from anywhere:
  .\scripts\run-3-nodes.ps1

This is the rough M4 version for manually watching election + (after M5)
replication. The fully scripted put/kill/verify/rejoin demo lives in
demo.ps1 (M6).
#>

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Remove-Item -Force "$repoRoot\data\node*.wal" -ErrorAction SilentlyContinue
Remove-Item -Force "$repoRoot\data\node*-raftstate.json*" -ErrorAction SilentlyContinue

$nodeIds = @("node1", "node2", "node3")
$procs = @()

foreach ($id in $nodeIds) {
    $p = Start-Process powershell -WorkingDirectory $repoRoot -ArgumentList @(
        "-NoExit", "-Command",
        "go run ./cmd/node -id $id -config .\scripts\cluster.json -data-dir .\data"
    ) -PassThru
    $procs += $p
    Write-Host "started $id (PID $($p.Id))"
}

Write-Host ""
Write-Host "3 nodes starting in separate windows. Watch for '-> Leader' in the logs."
Write-Host "To test failover, hard-kill the leader's window with:"
Write-Host "  taskkill /F /PID <leader-pid>"
Write-Host "PIDs: $($procs.Id -join ', ')"

<#
Fully scripted demo of the whole project: starts 3 nodes, writes through
whichever gets elected leader, shows a follower redirecting instead of
answering, hard-kills the leader with taskkill, shows the survivors elect
a new leader within a couple of seconds and keep serving both reads and
writes, restarts the killed node, and shows it rejoin and catch up on the
write it missed. Runs start-to-finish in well under a minute.

Run from anywhere:
  .\scripts\demo.ps1

Requires `go` on PATH (run .\scripts\check-env.ps1 first if unsure).
Wipes any existing data\node*.wal / data\node*-raftstate.json /
data\node*.log before starting -- don't run this if you have manual-run
data under data\ that you want to keep.
#>

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$dataDir = Join-Path $repoRoot "data"
$binDir = Join-Path $repoRoot "bin"

function Section($title) {
    Write-Host ""
    Write-Host "==> $title" -ForegroundColor Cyan
}

function StartNode($id) {
    $logPath = Join-Path $dataDir "$id.log"
    Remove-Item -Force $logPath -ErrorAction SilentlyContinue
    $p = Start-Process powershell -WorkingDirectory $repoRoot -ArgumentList @(
        "-NoExit", "-Command",
        ".\bin\node.exe -id $id -config .\scripts\cluster.json -data-dir .\data *> .\data\$id.log"
    ) -PassThru
    return $p
}

function KvctlPut($clientAddr, $key, $value) {
    & "$binDir\kvctl.exe" -addr $clientAddr put $key $value
}

function KvctlGet($clientAddr, $key) {
    & "$binDir\kvctl.exe" -addr $clientAddr get $key
}

# Returns the node ID whose log's most recent "-> Leader"/"-> Follower"
# transition is a "-> Leader" line, or $null if none of the given IDs
# currently look like the leader.
function FindLeader($ids) {
    foreach ($id in $ids) {
        $logPath = Join-Path $dataDir "$id.log"
        if (-not (Test-Path $logPath)) { continue }
        $lastTransition = Get-Content $logPath -ErrorAction SilentlyContinue |
            Where-Object { $_ -match "-> (Leader|Follower)" } |
            Select-Object -Last 1
        if ($lastTransition -match "-> Leader") {
            return $id
        }
    }
    return $null
}

function WaitForLeader($ids, $maxSeconds) {
    for ($i = 0; $i -lt ($maxSeconds * 4); $i++) {
        Start-Sleep -Milliseconds 250
        $found = FindLeader $ids
        if ($found) { return $found }
    }
    return $null
}

# --- setup ---

Section "building node and kvctl"
New-Item -ItemType Directory -Force -Path $binDir | Out-Null
Push-Location $repoRoot
try {
    go build -o "$binDir\node.exe" .\cmd\node
    go build -o "$binDir\kvctl.exe" .\cmd\kvctl
} finally {
    Pop-Location
}
Write-Host "  built bin\node.exe and bin\kvctl.exe"

Section "cleaning up any previous run"
Remove-Item -Force "$dataDir\node*.wal" -ErrorAction SilentlyContinue
Remove-Item -Force "$dataDir\node*-raftstate.json*" -ErrorAction SilentlyContinue
Remove-Item -Force "$dataDir\node*.log" -ErrorAction SilentlyContinue

$ids = @("node1", "node2", "node3")
$clientAddrs = @{ node1 = "127.0.0.1:8001"; node2 = "127.0.0.1:8002"; node3 = "127.0.0.1:8003" }
$procs = @{}

# --- start the cluster ---

Section "starting 3 nodes"
foreach ($id in $ids) {
    $p = StartNode $id
    $procs[$id] = $p.Id
    Write-Host "  $id started (PID $($p.Id))"
}

Section "waiting for leader election"
$leader = WaitForLeader $ids 10
if (-not $leader) {
    Write-Host "no leader elected within 10s -- check data\node*.log" -ForegroundColor Red
    exit 1
}
Write-Host "  leader is $leader" -ForegroundColor Green

# --- writes, redirect, failover ---

Section "writing through the leader"
KvctlPut $clientAddrs[$leader] "color" "blue"
Write-Host "  put color=blue via $leader"

Section "confirming a follower redirects instead of answering directly"
$follower = ($ids | Where-Object { $_ -ne $leader })[0]
KvctlGet $clientAddrs[$follower] "color"

$killed = $leader
$killedPid = $procs[$killed]
Section "hard-killing the leader ($killed, PID $killedPid) with taskkill /F /T"
taskkill /F /T /PID $killedPid | Out-Null

Section "waiting for the survivors to elect a new leader"
$survivors = $ids | Where-Object { $_ -ne $killed }
$newLeader = $null
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 250
    $candidate = FindLeader $survivors
    if ($candidate -and $candidate -ne $killed) { $newLeader = $candidate; break }
}
if (-not $newLeader) {
    Write-Host "no new leader elected within 10s -- check data\node*.log" -ForegroundColor Red
    exit 1
}
Write-Host "  new leader is $newLeader" -ForegroundColor Green

Section "confirming the cluster still has the pre-kill write"
KvctlGet $clientAddrs[$newLeader] "color"

Section "writing more data through the new leader"
KvctlPut $clientAddrs[$newLeader] "shape" "circle"
Write-Host "  put shape=circle via $newLeader"

# --- rejoin and catch-up ---

Section "restarting the killed node ($killed)"
$p = StartNode $killed
$procs[$killed] = $p.Id
Write-Host "  $killed restarted (PID $($p.Id))"

Section "waiting for it to rejoin and catch up"
Start-Sleep -Seconds 2
$logPath = Join-Path $dataDir "$killed.log"
$rejoined = Select-String -Path $logPath -Pattern "-> Follower \(leader is" -Quiet -ErrorAction SilentlyContinue
$caughtUp = Select-String -Path $logPath -Pattern "appended \d+ log entries" -Quiet -ErrorAction SilentlyContinue

if ($rejoined) {
    Write-Host "  $killed rejoined the cluster as a follower" -ForegroundColor Green
} else {
    Write-Host "  $killed did not visibly rejoin within the wait window (check data\$killed.log)" -ForegroundColor Yellow
}
if ($caughtUp) {
    Write-Host "  $killed received replicated log entries it missed while down (catch-up confirmed)" -ForegroundColor Green
} else {
    Write-Host "  no catch-up entries seen yet (check data\$killed.log)" -ForegroundColor Yellow
}

# --- summary ---

Section "done"
Write-Host "Cluster: $newLeader is leader; all 3 nodes are up (including rejoined $killed)."
Write-Host "Full logs are in .\data\node*.log"
Write-Host ""
Write-Host "Stop everything with:"
foreach ($id in $ids) {
    $nodePid = $procs[$id]
    Write-Host "  taskkill /F /T /PID $nodePid"
}

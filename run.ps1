<#
.SYNOPSIS
  Windows task runner for the worker-health stack. The PowerShell equivalent
  of the Makefile -- same target names, no make, no bash, no curl required.

.EXAMPLE
  .\run.ps1 up
  .\run.ps1 health
  .\run.ps1 chaos-db-blackhole
  .\run.ps1 restore
  .\run.ps1 down
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Command = "help",

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

# Every call this script makes is to localhost, but a system or corporate proxy
# will happily intercept those too and reject them (typically "User agent not
# allowed" or a 407). Bypass it for this process only, in both PowerShell
# editions -- 5.1 goes through WebRequest, 6+ through HttpClient, and they read
# completely different settings.
try { [System.Net.WebRequest]::DefaultWebProxy = $null } catch { }

$script:HttpExtra = @{}
if ($PSVersionTable.PSVersion.Major -ge 6) {
    # -NoProxy exists only on PowerShell 6+; on 5.1 the line above is enough.
    $script:HttpExtra['NoProxy'] = $true
}

# Port defaults mirror .env.example; a real .env overrides them.
$Ports = @{
    BILLING   = 8081
    NOTIFY    = 8082
    RECONCILE = 8083
    LOADGEN   = 8090
    DASHBOARD = 9000
    TOXIPROXY = 8474
}
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)_PORT\s*=\s*(\d+)\s*$') {
            $key = $Matches[1] -replace '_HOST$', ''
            if ($Ports.ContainsKey($key)) { $Ports[$key] = [int]$Matches[2] }
        }
    }
}

function Write-Head($text) { Write-Host "`n$text" -ForegroundColor Cyan }
function Write-Ok($text)   { Write-Host $text -ForegroundColor Green }
function Write-Warn($text) { Write-Host $text -ForegroundColor Yellow }

function Assert-Docker {
    $exe = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $exe) {
        throw "docker not found on PATH. Install Docker Desktop and make sure it is running."
    }
    try { docker info 2>&1 | Out-Null } catch { }
    if ($LASTEXITCODE -ne 0) {
        throw "The Docker daemon is not responding. Start Docker Desktop, wait for the whale icon to settle, then retry."
    }
}

function Invoke-Api {
    param([string]$Uri, [string]$Method = "GET", $Body = $null)
    $params = @{ Uri = $Uri; Method = $Method; TimeoutSec = 8 } + $script:HttpExtra
    if ($null -ne $Body) {
        $params.Body        = ($Body | ConvertTo-Json -Compress)
        $params.ContentType = "application/json"
    }
    return Invoke-RestMethod @params
}

function Get-WorkerHealth {
    param([string]$Name, [int]$Port)
    try {
        $h = Invoke-Api "http://localhost:$Port/health"
        $checks = ($h.checks.PSObject.Properties |
            ForEach-Object { "$($_.Name)=$($_.Value.internal_status)" }) -join "  "
        $delta = $h.timing.worker_to_health_delta_ms
        $d = if ($null -ne $delta) { "{0:N1} ms" -f $delta } else { "--" }
        $colour = switch ($h.status) {
            "ok"       { "Green" }
            "degraded" { "Yellow" }
            default    { "Red" }
        }
        Write-Host ("  {0,-10} {1,-9} runner={2,-8} delta={3,-9} {4}" -f `
            $Name, $h.status, $h.timing.runner, $d, $checks) -ForegroundColor $colour
    } catch {
        Write-Host ("  {0,-10} unreachable on :{1}" -f $Name, $Port) -ForegroundColor Red
    }
}

function Add-Toxic {
    param([string]$Proxy, [string]$ToxicName, [string]$Type, [hashtable]$Attributes)
    Invoke-Api "http://localhost:$($Ports.TOXIPROXY)/proxies/$Proxy/toxics" "POST" @{
        name = $ToxicName; type = $Type; stream = "downstream"
        toxicity = 1.0; attributes = $Attributes
    } | Out-Null
}

function Set-ProxyEnabled {
    param([string]$Proxy, [bool]$Enabled)
    Invoke-Api "http://localhost:$($Ports.TOXIPROXY)/proxies/$Proxy" "POST" @{ enabled = $Enabled } | Out-Null
}

function Set-Load {
    param($Payload)
    Invoke-Api "http://localhost:$($Ports.LOADGEN)/" "POST" $Payload | Out-Null
}

switch ($Command.ToLower()) {

    "build" { Assert-Docker; docker compose build }

    "up" {
        Assert-Docker
        docker compose build
        docker compose up -d
        Write-Head "Waiting for workers to report ready..."
        $deadline = (Get-Date).AddSeconds(150)
        do {
            Start-Sleep -Seconds 5
            $ready = 0
            foreach ($p in @($Ports.BILLING, $Ports.NOTIFY, $Ports.RECONCILE)) {
                try {
                    if ((Invoke-Api "http://localhost:$p/health").status -in @("ok", "degraded")) { $ready++ }
                } catch { }
            }
            Write-Host "  $ready/3 ready"
        } while ($ready -lt 3 -and (Get-Date) -lt $deadline)

        if ($ready -lt 3) {
            Write-Warn "`n  Only $ready/3 came up. Try: .\run.ps1 logs"
        } else {
            Write-Ok "`n  All three workers ready."
        }
        Write-Head "Open these:"
        Write-Host "  dashboard   http://localhost:$($Ports.DASHBOARD)"
        Write-Host "  billing     http://localhost:$($Ports.BILLING)/health"
        Write-Host "  notify      http://localhost:$($Ports.NOTIFY)/health"
        Write-Host "  reconcile   http://localhost:$($Ports.RECONCILE)/health"
        if ($ready -ge 3) { Start-Process "http://localhost:$($Ports.DASHBOARD)" }
    }

    "down"  { Assert-Docker; docker compose down -v --remove-orphans }
    "clean" { Assert-Docker; docker compose down -v --remove-orphans; docker compose rm -f }
    "ps"    { Assert-Docker; docker compose ps }
    "logs"  { Assert-Docker; docker compose logs -f billing notify reconcile }

    "test" {
        Assert-Docker
        docker compose --profile test run --rm tests pytest -q tests
    }

    "unit" {
        # Runs natively on Windows Python -- no containers, no Docker at all.
        # Written for Windows PowerShell 5.1, which ships with Windows and has
        # no null-coalescing operator.
        $py = Get-Command python -ErrorAction SilentlyContinue
        if (-not $py) { $py = Get-Command py -ErrorAction SilentlyContinue }
        if (-not $py) {
            throw "python not found on PATH. Install Python 3.11+, or run the suite in Docker with: .\run.ps1 test"
        }
        $env:PYTHONPATH = "src"
        & $py.Source -m pytest tests/unit -q
    }

    "health" {
        Write-Head "Fleet status"
        Get-WorkerHealth "billing"   $Ports.BILLING
        Get-WorkerHealth "notify"    $Ports.NOTIFY
        Get-WorkerHealth "reconcile" $Ports.RECONCILE
        Write-Host ""
    }

    "export" {
        $m = Invoke-RestMethod @script:HttpExtra -Uri "http://localhost:$($Ports.BILLING)/health" -TimeoutSec 8
        if ($m.export) { $m.export | ConvertTo-Json }
        else { Write-Host "OTLP export is not configured (set HEALTH_OTEL_ENDPOINT)" }
    }

    # ---- fault injection -------------------------------------------------- #
    "chaos-db-down"      { Set-ProxyEnabled "postgres" $false;    Write-Warn "postgres proxy DISABLED" }
    "chaos-mq-down"      { Set-ProxyEnabled "rabbitmq" $false;    Write-Warn "rabbitmq proxy DISABLED" }
    "chaos-redis-down"   { Set-ProxyEnabled "redis-cache" $false; Write-Warn "redis-cache proxy DISABLED (locks path stays up)" }

    "chaos-db-blackhole" {
        Add-Toxic "postgres" "blackhole" "timeout" @{ timeout = 0 }
        Write-Warn "postgres BLACK HOLE - packets dropped, socket stays open. This is the hard one."
    }
    "chaos-db-slow" {
        Add-Toxic "postgres" "slow" "latency" @{ latency = 400; jitter = 0 }
        Write-Warn "postgres +400ms (below the check timeout - should stay OK)"
    }
    "restore" {
        Invoke-Api "http://localhost:$($Ports.TOXIPROXY)/reset" "POST" | Out-Null
        Write-Ok "all proxies restored"
    }

    # ---- load control ------------------------------------------------------ #
    "idle"   { Set-Load @{ rate = 0 };     Write-Ok "load paused - a quiet queue must NOT alert" }
    "normal" { Set-Load @{ rate = 8 };     Write-Ok "load resumed at 8/s" }
    "burst"  { Set-Load @{ burst = 2000 }; Write-Ok "2000 messages queued" }

    default {
        @"
worker-health - Windows task runner

  .\run.ps1 up                   build and start everything, then open the dashboard
  .\run.ps1 down                 stop everything and remove volumes
  .\run.ps1 ps                   container status
  .\run.ps1 logs                 tail the three workers

  .\run.ps1 health               aggregate status of every worker
  .un.ps1 export               OTLP exporter counters from billing

  .\run.ps1 test                 full suite, in a container on the compose network
  .\run.ps1 unit                 unit tier only, native Python, no Docker

Fault injection - run these while watching the dashboard:

  .\run.ps1 chaos-db-blackhole   packets dropped, socket stays open
  .\run.ps1 chaos-db-down        port closed
  .\run.ps1 chaos-db-slow        +400ms, below the timeout
  .\run.ps1 chaos-redis-down     non-critical dependency: degrades, does not fail
  .\run.ps1 chaos-mq-down        broker unreachable
  .\run.ps1 restore              clear every fault

Load control:

  .\run.ps1 idle                 pause load - the quiet-queue test
  .\run.ps1 burst                dump 2000 messages
  .\run.ps1 normal               resume steady load
"@ | Write-Host
    }
}

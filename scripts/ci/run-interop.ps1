[CmdletBinding()]
param(
    [string]$Target = "",
    [string]$Protocol = "",
    [string]$Scenario = "",
    [switch]$SelfTest
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Expand-Selector([string]$Value) {
    @($Value.Split(',', [System.StringSplitOptions]::RemoveEmptyEntries) | ForEach-Object { $_.Trim().ToLowerInvariant() })
}

function Assert-Selector([string]$Name, [string[]]$Values, [string[]]$Allowed) {
    if ($Values.Count -eq 0) {
        throw "$Name selector is required"
    }
    foreach ($value in $Values) {
        if ($value -notin $Allowed) {
            throw "Invalid $Name selector '$value'. Allowed: $($Allowed -join ', ')"
        }
    }
}

function Get-RedactedText([string]$Value) {
    $Value -replace '(?i)(access_token|authorization)=?[^&\s]*', '$1=[REDACTED]'
}

function Invoke-SelfTest {
    $invalidRejected = $false
    try {
        Assert-Selector "target" @("invalid") @("native", "wasm")
    }
    catch {
        $invalidRejected = $true
    }
    if (-not $invalidRejected) {
        throw "Selector self-test did not reject an invalid target"
    }

    $probe = Join-Path ([System.IO.Path]::GetTempPath()) "rayx-signalr-interop-$PID.probe"
    $failurePropagated = $false
    try {
        [System.IO.File]::WriteAllText($probe, "owned")
        throw "deliberate fixture failure"
    }
    catch {
        $failurePropagated = $_.Exception.Message -eq "deliberate fixture failure"
    }
    finally {
        if ([System.IO.File]::Exists($probe)) {
            [System.IO.File]::Delete($probe)
        }
    }
    if (-not $failurePropagated -or [System.IO.File]::Exists($probe)) {
        throw "Failure propagation or finally cleanup self-test failed"
    }

    $redacted = Get-RedactedText "wss://localhost/test?access_token=secret Authorization=Bearer-secret"
    if ($redacted -match 'secret') {
        throw "Diagnostic redaction self-test failed"
    }
    Write-Host "Interop harness self-test passed"
}

if ($SelfTest) {
    Invoke-SelfTest
    exit 0
}

$targets = Expand-Selector $Target
$protocols = Expand-Selector $Protocol
$scenarios = Expand-Selector $Scenario
Assert-Selector "target" $targets @("native", "wasm")
Assert-Selector "protocol" $protocols @("json", "messagepack")
Assert-Selector "scenario" $scenarios @("ordinary-reconnect", "stateful-reconnect")

$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$port = if ($env:RAYX_SIGNALR_INTEROP_PORT) { [int]$env:RAYX_SIGNALR_INTEROP_PORT } else { 5220 }
$service = $null
$previousUrls = $env:ASPNETCORE_URLS

try {
    $env:ASPNETCORE_URLS = "http://127.0.0.1:$port"
    $service = Start-Process dotnet -ArgumentList @(
        "run", "--project", "dotnet/SignalRTestService/SignalRTestService.csproj", "--no-build"
    ) -WorkingDirectory $root -PassThru -WindowStyle Hidden

    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        if ($service.HasExited) {
            throw "SignalR fixture exited before becoming healthy"
        }
        try {
            $health = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/health" -TimeoutSec 2
            if ($health.StatusCode -eq 200) { break }
        }
        catch {
            Start-Sleep -Milliseconds 200
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    if ([DateTime]::UtcNow -ge $deadline) {
        throw "SignalR fixture health check timed out"
    }

    foreach ($targetName in $targets) {
        foreach ($protocolName in $protocols) {
            foreach ($scenarioName in $scenarios) {
                Write-Host "Running target=$targetName protocol=$protocolName scenario=$scenarioName"
                if ($targetName -eq "native") {
                    $filter = "interop_$($scenarioName.Replace('-', '_'))_$protocolName"
                    & cargo test --lib $filter --all-features
                }
                else {
                    $grep = "$($scenarioName.Replace('-', ' ')) $protocolName"
                    & pnpm --dir=tools-node exec playwright test --project=chromium --grep $grep --workers=1
                }
                if ($LASTEXITCODE -ne 0) {
                    throw "Interop scenario failed with exit code $LASTEXITCODE"
                }
            }
        }
    }
}
catch {
    throw (Get-RedactedText $_.Exception.Message)
}
finally {
    $env:ASPNETCORE_URLS = $previousUrls
    if ($null -ne $service -and -not $service.HasExited) {
        Stop-Process -Id $service.Id -Force
        $service.WaitForExit(5000) | Out-Null
    }
}

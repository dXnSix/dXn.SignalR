[CmdletBinding()]
param(
    [string]$Target = "",
    [string]$Protocol = "",
    [string]$Scenario = "",
    [int]$ScenarioTimeoutSeconds = 60,
    [string]$DiagnosticsDirectory = "artifacts-temp/interop",
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

function Get-RedactedText([AllowEmptyString()][string]$Value) {
    $redacted = $Value -replace '(?i)(access_token=)[^&\s]+', '$1[REDACTED]'
    $redacted = $redacted -replace '(?i)(authorization\s*[:=]\s*)[^\r\n]+', '$1[REDACTED]'
    $redacted = $redacted -replace '(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+', '$1[REDACTED]'
    $redacted
}

function Get-BoundedTail([AllowEmptyString()][string]$Value, [int]$MaximumCharacters = 32768) {
    if ($Value.Length -le $MaximumCharacters) {
        return $Value
    }
    $Value.Substring($Value.Length - $MaximumCharacters)
}

function Read-SharedText([string]$Path) {
    if (-not [System.IO.File]::Exists($Path)) {
        return ""
    }

    $stream = [System.IO.FileStream]::new(
        $Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::ReadWrite
    )
    $reader = [System.IO.StreamReader]::new($stream)
    try {
        $reader.ReadToEnd()
    }
    finally {
        $reader.Dispose()
    }
}

$script:Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$script:DiagnosticsPath = Join-Path $script:Root $DiagnosticsDirectory

function Write-FailureArtifact(
    [string]$Cell,
    [string]$Message,
    [int]$ExitCode,
    [bool]$TimedOut,
    [AllowEmptyString()][string]$StandardOutput,
    [AllowEmptyString()][string]$StandardError,
    [AllowEmptyString()][string]$ServiceOutput = "",
    [AllowEmptyString()][string]$ServiceError = ""
) {
    [System.IO.Directory]::CreateDirectory($script:DiagnosticsPath) | Out-Null
    $safeCell = $Cell -replace '[^A-Za-z0-9_.-]', '-'
    $path = Join-Path $script:DiagnosticsPath "$safeCell.json"
    $payload = [ordered]@{
        schemaVersion = 1
        cell = $Cell
        message = Get-RedactedText $Message
        exitCode = $ExitCode
        timedOut = $TimedOut
        stdoutTail = Get-BoundedTail (Get-RedactedText $StandardOutput)
        stderrTail = Get-BoundedTail (Get-RedactedText $StandardError)
        serviceStdoutTail = Get-BoundedTail (Get-RedactedText $ServiceOutput)
        serviceStderrTail = Get-BoundedTail (Get-RedactedText $ServiceError)
    }
    [System.IO.File]::WriteAllText($path, ($payload | ConvertTo-Json -Depth 4), [System.Text.UTF8Encoding]::new($false))
    Write-Host "Failure diagnostics: $path"
}

function Invoke-BoundedCommand(
    [string]$Executable,
    [string[]]$Arguments,
    [int]$TimeoutSeconds,
    [string]$Cell
) {
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Executable
    $startInfo.WorkingDirectory = $script:Root
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        $startInfo.ArgumentList.Add($argument)
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw "Failed to start $Executable"
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timedOut = -not $process.WaitForExit($TimeoutSeconds * 1000)
        if ($timedOut) {
            try {
                $process.Kill($true)
            }
            catch {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
        }
        $process.WaitForExit()
        $standardOutput = $stdoutTask.GetAwaiter().GetResult()
        $standardError = $stderrTask.GetAwaiter().GetResult()
        $exitCode = if ($timedOut) { 124 } else { $process.ExitCode }

        $safeOutput = Get-RedactedText $standardOutput
        $safeError = Get-RedactedText $standardError
        if ($safeOutput) {
            Write-Host $safeOutput.TrimEnd()
        }
        if ($safeError) {
            [Console]::Error.WriteLine($safeError.TrimEnd())
        }

        if ($timedOut -or $exitCode -ne 0) {
            $message = if ($timedOut) {
                "$Cell exceeded the $TimeoutSeconds-second timeout"
            }
            else {
                "$Cell failed with exit code $exitCode"
            }
            Write-FailureArtifact $Cell $message $exitCode $timedOut $standardOutput $standardError
            throw $message
        }

        [pscustomobject]@{
            ExitCode = $exitCode
            Output = "$standardOutput`n$standardError"
        }
    }
    finally {
        $process.Dispose()
    }
}

function Assert-ScenarioExecuted([string]$Cell, [string]$Output, [string]$Pattern) {
    if ($Output -notmatch $Pattern) {
        $message = "$Cell completed without executing exactly one selected scenario"
        Write-FailureArtifact $Cell $message 1 $false $Output "Expected output pattern: $Pattern"
        throw $message
    }
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
    if ((Get-BoundedTail ("x" * 128) 64).Length -ne 64) {
        throw "Bounded diagnostic self-test failed"
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
Assert-Selector "scenario" $scenarios @("ordinary-reconnect", "stateful-reconnect", "short-stream-chaining")
if ($targets -contains "native" -and $scenarios -contains "short-stream-chaining") {
    throw "The short-stream-chaining scenario is browser-WASM-specific"
}
if ($ScenarioTimeoutSeconds -lt 1 -or $ScenarioTimeoutSeconds -gt 60) {
    throw "ScenarioTimeoutSeconds must be between 1 and 60"
}

$port = if ($env:RAYX_SIGNALR_INTEROP_PORT) { [int]$env:RAYX_SIGNALR_INTEROP_PORT } else { 5220 }
if ($port -ne 5220) {
    throw "The checked-in interop clients require the isolated fixture port 5220"
}

$service = $null
$previousUrls = $env:ASPNETCORE_URLS
$serviceOutputPath = [System.IO.Path]::GetTempFileName()
$serviceErrorPath = [System.IO.Path]::GetTempFileName()
$failureMessage = ""
$cleanupFailure = ""
try {
    Invoke-BoundedCommand "dotnet" @(
        "build", "dotnet/SignalRTestService/SignalRTestService.csproj", "--nologo"
    ) 180 "bootstrap-dotnet" | Out-Null

    if ($targets -contains "native") {
        Invoke-BoundedCommand "cargo" @(
            "test", "--lib", "--all-features", "--no-run"
        ) 180 "bootstrap-native" | Out-Null
    }
    if ($targets -contains "wasm") {
        Invoke-BoundedCommand "node" @(
            "tools-node/scripts/build-wasm-fixture.mjs"
        ) 180 "bootstrap-wasm" | Out-Null
    }

    $env:ASPNETCORE_URLS = "http://127.0.0.1:$port"
    $serviceArguments = @(
        "run", "--project", "dotnet/SignalRTestService/SignalRTestService.csproj", "--no-build", "--no-launch-profile"
    )
    $startParameters = @{
        FilePath = "dotnet"
        ArgumentList = $serviceArguments
        WorkingDirectory = $script:Root
        PassThru = $true
        RedirectStandardOutput = $serviceOutputPath
        RedirectStandardError = $serviceErrorPath
    }
    if ($IsWindows) {
        $startParameters.WindowStyle = "Hidden"
    }
    $service = Start-Process @startParameters

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
                $cell = "$targetName-$protocolName-$scenarioName"
                Write-Host "Running target=$targetName protocol=$protocolName scenario=$scenarioName"
                if ($targetName -eq "native") {
                    $filter = "interop_$($scenarioName.Replace('-', '_'))_$protocolName"
                    $result = Invoke-BoundedCommand "cargo" @(
                        "test", "--lib", $filter, "--all-features"
                    ) $ScenarioTimeoutSeconds $cell
                    Assert-ScenarioExecuted $cell $result.Output '(?m)test result: ok\. 1 passed; 0 failed;'
                }
                else {
                    $grep = "$($scenarioName.Replace('-', ' ')) $protocolName"
                    $result = Invoke-BoundedCommand "node" @(
                        "tools-node/node_modules/playwright/cli.js", "test", "--config=tools-node/playwright.config.ts", "--project=chromium", "--grep", $grep, "--workers=1"
                    ) $ScenarioTimeoutSeconds $cell
                    Assert-ScenarioExecuted $cell $result.Output '(?m)\b1 passed\b'
                }
            }
        }
    }
}
catch {
    $failureMessage = Get-RedactedText $_.Exception.Message
    $serviceOutput = Read-SharedText $serviceOutputPath
    $serviceError = Read-SharedText $serviceErrorPath
    Write-FailureArtifact "interop-harness" $failureMessage 1 $false "" "" $serviceOutput $serviceError
}
finally {
    $env:ASPNETCORE_URLS = $previousUrls
    if ($null -ne $service -and -not $service.HasExited) {
        try {
            $service.Kill($true)
            if (-not $service.WaitForExit(5000)) {
                $cleanupFailure = "SignalR fixture did not terminate within five seconds"
            }
        }
        catch {
            $cleanupFailure = Get-RedactedText $_.Exception.Message
        }
    }
    foreach ($temporaryPath in @($serviceOutputPath, $serviceErrorPath)) {
        if ([System.IO.File]::Exists($temporaryPath)) {
            [System.IO.File]::Delete($temporaryPath)
        }
    }
}

if ($cleanupFailure) {
    Write-FailureArtifact "interop-cleanup" $cleanupFailure 1 $false "" ""
    if (-not $failureMessage) {
        $failureMessage = $cleanupFailure
    }
}
if ($failureMessage) {
    throw $failureMessage
}

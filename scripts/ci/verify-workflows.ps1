[CmdletBinding()]
param(
    [switch]$Security,
    [switch]$RunLocalQuality
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

function Read-RequiredFile([string]$RelativePath) {
    $path = Join-Path $root $RelativePath
    if (-not [System.IO.File]::Exists($path)) {
        throw "Required workflow file is missing: $RelativePath"
    }
    [System.IO.File]::ReadAllText($path)
}

function Assert-Matches([string]$Name, [string]$Value, [string]$Pattern) {
    if ($Value -notmatch $Pattern) {
        throw "$Name is missing required contract pattern: $Pattern"
    }
}

function Assert-NotMatches([string]$Name, [string]$Value, [string]$Pattern) {
    if ($Value -match $Pattern) {
        throw "$Name contains prohibited contract pattern: $Pattern"
    }
}

function Invoke-Checked([string]$Description, [string]$Executable, [string[]]$Arguments) {
    Write-Host $Description
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

$quality = Read-RequiredFile ".github/workflows/quality.yml"
$interop = Read-RequiredFile ".github/workflows/interop.yml"
$harness = Read-RequiredFile "scripts/ci/run-interop.ps1"
$readme = Read-RequiredFile "README.md"
$forkNotes = Read-RequiredFile "FORK.md"
$allWorkflows = "$quality`n$interop"

foreach ($entry in @(
    @{ Name = "quality workflow"; Value = $quality },
    @{ Name = "interop workflow"; Value = $interop }
)) {
    Assert-Matches $entry.Name $entry.Value '(?ms)^on:\s*.*?pull_request:'
    Assert-Matches $entry.Name $entry.Value '(?ms)^on:\s*.*?push:'
    Assert-Matches $entry.Name $entry.Value '(?ms)^permissions:\s*\r?\n\s+contents:\s*read\s*$'
    Assert-Matches $entry.Name $entry.Value '(?ms)^concurrency:\s*.*?cancel-in-progress:\s*true'
    Assert-NotMatches $entry.Name $entry.Value '(?im)^\s*[A-Za-z-]+:\s*write\s*$'
    Assert-NotMatches $entry.Name $entry.Value '\$\{\{\s*secrets\.'
    Assert-NotMatches $entry.Name $entry.Value '(?i)publish|nuget\s+push|cargo\s+publish|pull_request_target'
}

Assert-Matches "quality workflow" $quality 'quality / rustfmt'
Assert-Matches "quality workflow" $quality 'cargo \+1\.92\.0 fmt --all --check'
Assert-Matches "quality workflow" $quality 'quality / clippy'
Assert-Matches "quality workflow" $quality 'clippy --all-targets --all-features -- -D warnings'
Assert-Matches "quality workflow" $quality 'os: \[ubuntu-latest, windows-latest, macos-latest\]'
Assert-Matches "quality workflow" $quality 'toolchain: \[1\.92\.0, stable\]'
Assert-Matches "quality workflow" $quality 'wasm32-unknown-unknown'
Assert-Matches "quality workflow" $quality 'mode: default'
Assert-Matches "quality workflow" $quality 'mode: all-features'
Assert-Matches "quality workflow" $quality 'quality / unit-contract'
Assert-Matches "quality workflow" $quality '--no-default-features'
Assert-Matches "quality workflow" $quality '--all-features'
Assert-Matches "quality workflow" $quality 'quality / rustdoc'
Assert-Matches "quality workflow" $quality 'doc --all-features --no-deps'
Assert-Matches "quality workflow" $quality 'quality / dependency-security'
Assert-Matches "quality workflow" $quality 'verify-workflows\.ps1 -Security'

Assert-Matches "interop workflow" $interop '(?m)^\s+workflow_dispatch:'
Assert-Matches "interop workflow" $interop 'protocol: \[json, messagepack\]'
Assert-Matches "interop workflow" $interop 'scenario: \[ordinary-reconnect, stateful-reconnect, stream-cancellation\]'
Assert-Matches "interop workflow" $interop 'scenario: \[ordinary-reconnect, stateful-reconnect, short-stream-chaining, stream-cancellation\]'
Assert-Matches "interop workflow" $interop 'interop / native'
Assert-Matches "interop workflow" $interop 'os: \[ubuntu-latest, windows-latest, macos-latest\]'
Assert-Matches "interop workflow" $interop 'interop / Chromium WASM'
Assert-Matches "interop workflow" $interop 'playwright install --with-deps chromium'
Assert-Matches "interop workflow" $interop '-ScenarioTimeoutSeconds 60'
Assert-Matches "interop workflow" $interop '(?m)^\s+if: failure\(\)\s*$'
Assert-Matches "interop workflow" $interop 'actions/upload-artifact@v7'
Assert-Matches "interop workflow" $interop 'artifacts-temp/interop'
Assert-Matches "interop workflow" $interop 'retention-days: 7'

Assert-Matches "workflow actions" $allWorkflows 'actions/checkout@v7'
Assert-Matches "workflow actions" $allWorkflows 'actions/cache@v6'
Assert-Matches "workflow actions" $allWorkflows 'actions/setup-dotnet@v6'
Assert-Matches "browser workflow" $interop 'actions/setup-node@v6'

Assert-Matches "interop harness" $harness 'ScenarioTimeoutSeconds = 60'
Assert-Matches "interop harness" $harness 'stream-cancellation'
Assert-Matches "interop harness" $harness 'Assert-ScenarioExecuted'
Assert-Matches "interop harness" $harness 'Get-RedactedText'
Assert-Matches "interop harness" $harness 'Write-FailureArtifact'
Assert-Matches "interop harness" $harness '\.Kill\(\$true\)'
Assert-Matches "interop harness" $harness 'finally\s*\{'
Assert-Matches "interop harness" $harness 'playwright'
Assert-NotMatches "interop harness" $harness '(?i)webdriver|chromedriver|selenium'

Assert-Matches "README" $readme '## Production validation'
Assert-Matches "README" $readme 'quality / rustfmt'
Assert-Matches "README" $readme 'interop / Chromium WASM'
Assert-Matches "README" $readme 'verify-workflows\.ps1 -RunLocalQuality'
Assert-Matches "README" $readme 'run-interop\.ps1 -Target native '
Assert-Matches "README" $readme 'run-interop\.ps1 -Target wasm '
Assert-Matches "fork notes" $forkNotes '## Production hardening baseline'

Invoke-Checked "Interop harness contract self-test" "pwsh" @(
    "-NoProfile", "-File", (Join-Path $root "scripts/ci/run-interop.ps1"), "-SelfTest"
)

if ($Security) {
    if (-not (Get-Command cargo-audit -ErrorAction SilentlyContinue)) {
        throw "cargo-audit is missing; install it with: cargo install cargo-audit --locked"
    }
    Invoke-Checked "Generate the Rust dependency lock graph" "cargo" @("generate-lockfile")
    Invoke-Checked "RustSec vulnerability scan" "cargo" @("audit")

    Write-Host "NuGet transitive vulnerability scan"
    $nugetOutput = & dotnet list (Join-Path $root "dotnet/SignalRTestService/SignalRTestService.csproj") package --vulnerable --include-transitive 2>&1 | Out-String
    Write-Host $nugetOutput.TrimEnd()
    if ($LASTEXITCODE -ne 0) {
        throw "NuGet vulnerability scan failed with exit code $LASTEXITCODE"
    }
    if ($nugetOutput -notmatch '(?i)has no vulnerable packages') {
        throw "NuGet vulnerability scan did not prove a zero-vulnerability result"
    }
}

if ($RunLocalQuality) {
    Invoke-Checked "rustfmt" "cargo" @("fmt", "--all", "--check")
    Invoke-Checked "Clippy with warnings denied" "cargo" @("clippy", "--all-targets", "--all-features", "--", "-D", "warnings")

    $previousRustFlags = $env:RUSTFLAGS
    $previousRustdocFlags = $env:RUSTDOCFLAGS
    try {
        $env:RUSTFLAGS = "-D warnings"
        Invoke-Checked "Default-feature WASM check" "cargo" @("check", "--target", "wasm32-unknown-unknown")
        Invoke-Checked "All-feature WASM check" "cargo" @("check", "--target", "wasm32-unknown-unknown", "--all-features")

        $skipNetworkTests = @(
            "--skip", "tests::tests_tokio::test_service",
            "--skip", "tests::tests_tokio::interop_ordinary_reconnect_json",
            "--skip", "tests::tests_tokio::interop_stateful_reconnect_json",
            "--skip", "tests::tests_msgpack::test_msgpack_service",
            "--skip", "tests::tests_msgpack::interop_ordinary_reconnect_messagepack",
            "--skip", "tests::tests_msgpack::interop_stateful_reconnect_messagepack"
        )
        Invoke-Checked "JSON unit and contract tests" "cargo" (@("test", "--lib", "--no-default-features", "--") + $skipNetworkTests)
        Invoke-Checked "MessagePack unit and contract tests" "cargo" (@("test", "--lib", "--all-features", "--") + $skipNetworkTests)

        $env:RUSTDOCFLAGS = "-D warnings"
        Invoke-Checked "rustdoc with warnings denied" "cargo" @("doc", "--all-features", "--no-deps")
    }
    finally {
        $env:RUSTFLAGS = $previousRustFlags
        $env:RUSTDOCFLAGS = $previousRustdocFlags
    }
}

Write-Host "Workflow contract verification passed"

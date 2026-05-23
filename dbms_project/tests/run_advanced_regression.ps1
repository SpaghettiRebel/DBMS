param(
    [string]$ServerExe = "",
    [string]$DataRoot = "",
    [string]$HostName = "127.0.0.1",
    [int]$Port = 18090
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$CMakeCache = Join-Path $ProjectRoot "cmake-build-debug\CMakeCache.txt"

if (Test-Path $CMakeCache) {
    $compilerLine = Get-Content $CMakeCache | Where-Object { $_ -like "CMAKE_CXX_COMPILER:FILEPATH=*" } | Select-Object -First 1
    if ($compilerLine) {
        $compilerPath = $compilerLine.Substring($compilerLine.IndexOf("=") + 1)
        $compilerDir = Split-Path $compilerPath -Parent
        if (Test-Path $compilerDir) {
            $env:PATH = "$compilerDir;$env:PATH"
        }
    }
}

if ([string]::IsNullOrWhiteSpace($ServerExe)) {
    $ServerExe = Join-Path $ProjectRoot "cmake-build-debug\server\dbms_http_server.exe"
}

if (-not (Test-Path $ServerExe)) {
    throw "dbms_http_server.exe not found. Pass -ServerExe with the built server path."
}

$ServerExe = (Resolve-Path $ServerExe).Path

$createdTempDataRoot = $false
if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    $DataRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("dbms_advanced_" + [System.Guid]::NewGuid().ToString("N"))
    $createdTempDataRoot = $true
}

New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
$DataRoot = (Resolve-Path $DataRoot).Path
$AccountsFile = Join-Path $DataRoot "accounts.json"

$baseUri = "http://${HostName}:$Port"

function Invoke-JsonPost {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Body,
        [string]$Token = "",
        [int]$ExpectedStatus = 200
    )

    $headers = @{}
    if (-not [string]::IsNullOrWhiteSpace($Token)) {
        $headers["Authorization"] = "Bearer $Token"
    }

    try {
        $response = Invoke-WebRequest -Uri "$baseUri$Path" -Method Post -Headers $headers `
            -ContentType "application/json" -Body ($Body | ConvertTo-Json -Compress) -UseBasicParsing
    } catch {
        if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq $ExpectedStatus) {
            return ""
        }
        throw
    }

    if ($response.StatusCode -ne $ExpectedStatus) {
        throw "$Path returned $($response.StatusCode), expected $ExpectedStatus. Body: $($response.Content)"
    }
    return $response.Content
}

function Invoke-JsonGet {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [string]$Token = ""
    )

    $headers = @{}
    if (-not [string]::IsNullOrWhiteSpace($Token)) {
        $headers["Authorization"] = "Bearer $Token"
    }

    return (Invoke-WebRequest -Uri "$baseUri$Path" -Method Get -Headers $headers -UseBasicParsing).Content
}

function Invoke-DbSql {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][string]$Token
    )
    return Invoke-JsonPost "/query" @{ query = $Sql } $Token
}

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$Needle
    )

    if (-not $Text.Contains($Needle)) {
        throw "$Label failed. Expected to find '$Needle' in: $Text"
    }

    Write-Host "[PASS] $Label"
}

Write-Host "Starting advanced server: $ServerExe"
$server = Start-Process -FilePath $ServerExe `
    -ArgumentList @("--data-root", $DataRoot, "--host", $HostName, "--port", $Port, "--cluster-shards", "2", "--require-auth", "--accounts-file", $AccountsFile) `
    -PassThru -WindowStyle Hidden

try {
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($server.HasExited) {
            throw "Server exited before health check. Exit code: $($server.ExitCode)"
        }
        try {
            Invoke-WebRequest -Uri "$baseUri/health" -UseBasicParsing | Out-Null
            $ready = $true
            break
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    if (-not $ready) {
        throw "Server did not become ready"
    }

    $adminLogin = Invoke-JsonPost "/login" @{ login = "admin"; password = "admin" }
    $adminToken = (@($adminLogin | ConvertFrom-Json)[0]).token

    $dbName = "advanced_" + $PID
    Invoke-JsonPost "/accounts/create" @{ username = "reader"; password = "reader"; is_admin = $false } $adminToken | Out-Null
    Invoke-JsonPost "/permissions/set" @{
        database = "default"
        target_type = "user"
        target_name = "reader"
        permissions = @{ read = $true; write = $false; create_table = $false; drop_table = $false; drop_database = $false }
    } $adminToken | Out-Null
    Invoke-JsonPost "/permissions/set" @{
        database = $dbName
        target_type = "user"
        target_name = "reader"
        permissions = @{ read = $true; write = $false; create_table = $false; drop_table = $false; drop_database = $false }
    } $adminToken | Out-Null
    $readerLogin = Invoke-JsonPost "/login" @{ login = "reader"; password = "reader" }
    $readerToken = (@($readerLogin | ConvertFrom-Json)[0]).token

    Invoke-DbSql "CREATE DATABASE $dbName;" $adminToken | Out-Null
    Invoke-DbSql "USE $dbName;" $adminToken | Out-Null
    Invoke-DbSql 'CREATE TABLE users (id INT INDEXED, name STRING, age INT);' $adminToken | Out-Null
    Invoke-DbSql 'INSERT INTO users (id, name, age) VALUES (1, "Ann", 20), (2, "Bob", 30);' $adminToken | Out-Null

    Assert-Contains "reader can select" (Invoke-DbSql 'SELECT COUNT(*) AS total FROM users;' $readerToken) '"total":2'
    Invoke-JsonPost "/query" @{ query = 'UPDATE users SET age = 31 WHERE id == 2;' } $readerToken 403 | Out-Null
    Write-Host "[PASS] reader write forbidden"

    $asyncCreated = Invoke-JsonPost "/query_async" @{ query = 'SELECT SUM(age) AS age_sum FROM users;' } $adminToken 202
    $requestId = (@($asyncCreated | ConvertFrom-Json)[0]).request_id
    $asyncResult = ""
    for ($i = 0; $i -lt 20; $i++) {
        $asyncResult = Invoke-JsonGet "/async/$requestId" $adminToken
        if ($asyncResult.Contains('"completed"')) { break }
        Start-Sleep -Milliseconds 250
    }
    Assert-Contains "async completed" $asyncResult '"completed"'
    Assert-Contains "async result" $asyncResult '"age_sum":50'

    Assert-Contains "cluster nodes" (Invoke-JsonGet "/cluster/nodes" $adminToken) '"node_id":"shard_'
    Assert-Contains "cluster stats" (Invoke-JsonGet "/cluster/stats" $adminToken) '"mode":"sharded"'

    Write-Host "Advanced regression checks passed."
} finally {
    if ($server -and -not $server.HasExited) {
        Stop-Process -Id $server.Id -Force
        Wait-Process -Id $server.Id -Timeout 5 -ErrorAction SilentlyContinue
    }

    if ($createdTempDataRoot -and (Test-Path $DataRoot)) {
        for ($attempt = 1; $attempt -le 10; $attempt++) {
            try {
                Remove-Item -LiteralPath $DataRoot -Recurse -Force
                break
            } catch {
                Start-Sleep -Milliseconds 300
            }
        }
    }
}

param(
    [string]$ServerExe = "",
    [string]$DataRoot = "",
    [string]$HostName = "127.0.0.1",
    [int]$Port = 18080
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
    $candidates = @(
        (Join-Path $ProjectRoot "cmake-build-debug\server\dbms_http_server.exe"),
        (Join-Path $ProjectRoot "build\server\dbms_http_server.exe")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            $ServerExe = $candidate
            break
        }
    }
}

if ([string]::IsNullOrWhiteSpace($ServerExe) -or -not (Test-Path $ServerExe)) {
    throw "dbms_http_server.exe not found. Pass -ServerExe with the built server path."
}

$ServerExe = (Resolve-Path $ServerExe).Path

$createdTempDataRoot = $false
if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    $DataRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("dbms_regression_" + [System.Guid]::NewGuid().ToString("N"))
    $createdTempDataRoot = $true
}

New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
$DataRoot = (Resolve-Path $DataRoot).Path

$baseUri = "http://${HostName}:$Port"
$queryUri = "$baseUri/query"
$healthUri = "$baseUri/health"

function Invoke-DbSql {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $body = @{ query = $Sql } | ConvertTo-Json -Compress
    $response = Invoke-WebRequest -Uri $queryUri -Method Post -ContentType "application/json" -Body $body -UseBasicParsing
    return $response.Content
}

function Convert-ToCanonicalJson {
    param([Parameter(Mandatory = $true)][string]$JsonText)

    $parsed = $JsonText | ConvertFrom-Json
    return ($parsed | ConvertTo-Json -Compress -Depth 32)
}

function Assert-JsonEquals {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$ActualJson,
        [Parameter(Mandatory = $true)][string]$ExpectedJson
    )

    $actual = Convert-ToCanonicalJson $ActualJson
    $expected = Convert-ToCanonicalJson $ExpectedJson

    if ($actual -ne $expected) {
        throw "$Label failed.`nExpected: $expected`nActual:   $actual"
    }

    Write-Host "[PASS] $Label"
}

function Assert-FirstNumber {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$ActualJson,
        [Parameter(Mandatory = $true)][string]$Field,
        [Parameter(Mandatory = $true)][double]$Expected,
        [double]$Tolerance = 0.000001
    )

    $rows = @($ActualJson | ConvertFrom-Json)
    if ($rows.Count -lt 1) {
        throw "$Label failed. Empty result."
    }

    $actual = [double]$rows[0].$Field
    if ([Math]::Abs($actual - $Expected) -gt $Tolerance) {
        throw "$Label failed. Expected $Field=$Expected, actual $actual"
    }

    Write-Host "[PASS] $Label"
}

Write-Host "Starting server: $ServerExe"
$server = Start-Process -FilePath $ServerExe `
    -ArgumentList @("--data-root", $DataRoot, "--host", $HostName, "--port", $Port) `
    -PassThru -WindowStyle Hidden

try {
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($server.HasExited) {
            throw "Server exited before health check. Exit code: $($server.ExitCode)"
        }

        try {
            Invoke-WebRequest -Uri $healthUri -UseBasicParsing | Out-Null
            $ready = $true
            break
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }

    if (-not $ready) {
        throw "Server did not become ready at $healthUri"
    }

    $dbName = "regression_" + $PID

    Assert-JsonEquals "create database" (Invoke-DbSql "CREATE DATABASE $dbName;") '[{"status":"ok"}]'
    Assert-JsonEquals "use database" (Invoke-DbSql "USE $dbName;") '[{"status":"ok"}]'
    Assert-JsonEquals "create users" (Invoke-DbSql 'CREATE TABLE users (id INT INDEXED, name STRING, age INT, city STRING);') '[{"status":"ok"}]'
    Assert-JsonEquals "insert users" (Invoke-DbSql 'INSERT INTO users (id, name, age, city) VALUES (1, "Ann", 20, "Moscow"), (2, "Bob", 30, "Kazan"), (3, "Kate", 22, "Moscow");') '[{"status":"ok"}]'

    Assert-JsonEquals "aliases with indexed where" `
        (Invoke-DbSql 'SELECT id AS user_id, name AS user_name FROM users WHERE id == 2;') `
        '[{"user_id":2,"user_name":"Bob"}]'

    Assert-JsonEquals "projection aliases with order" `
        (Invoke-DbSql 'SELECT name AS user_name, age AS user_age FROM users WHERE id >= 2 ORDER BY id;') `
        '[{"user_age":30,"user_name":"Bob"},{"user_age":22,"user_name":"Kate"}]'

    Assert-JsonEquals "update multi column" `
        (Invoke-DbSql 'UPDATE users SET age = 31, name = "Robert" WHERE id == 2;') `
        '[{"status":"ok"}]'

    Assert-JsonEquals "updated row" `
        (Invoke-DbSql 'SELECT id, name, age FROM users WHERE id == 2;') `
        '[{"age":31,"id":2,"name":"Robert"}]'

    Assert-JsonEquals "count aggregate alias" `
        (Invoke-DbSql 'SELECT COUNT(*) AS total FROM users;') `
        '[{"total":3}]'

    Assert-JsonEquals "sum aggregate alias" `
        (Invoke-DbSql 'SELECT SUM(age) AS age_sum FROM users;') `
        '[{"age_sum":73}]'

    Assert-FirstNumber "avg aggregate alias" `
        (Invoke-DbSql 'SELECT AVG(age) AS avg_age FROM users WHERE id == 1;') `
        "avg_age" 20

    Assert-JsonEquals "group by aggregate aliases" `
        (Invoke-DbSql 'SELECT city, COUNT(*) AS users_count FROM users GROUP BY city ORDER BY city;') `
        '[{"city":"Kazan","users_count":1},{"city":"Moscow","users_count":2}]'

    Assert-JsonEquals "and like condition" `
        (Invoke-DbSql 'SELECT id AS matched_id FROM users WHERE age >= 20 AND city LIKE "Mos%";') `
        '[{"matched_id":1},{"matched_id":3}]'

    Assert-JsonEquals "delete row" `
        (Invoke-DbSql 'DELETE FROM users WHERE id == 1;') `
        '[{"status":"ok"}]'

    Assert-JsonEquals "after delete" `
        (Invoke-DbSql 'SELECT id, name FROM users ORDER BY id;') `
        '[{"id":2,"name":"Robert"},{"id":3,"name":"Kate"}]'

    Assert-JsonEquals "revert before data" `
        (Invoke-DbSql 'REVERT users "2000.01.01-00:00:00.000";') `
        '[{"status":"ok"}]'

    Assert-JsonEquals "after revert" `
        (Invoke-DbSql 'SELECT * FROM users;') `
        '[]'

    Write-Host "All regression checks passed."
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
                if ($attempt -eq 10) {
                    Write-Warning "Could not remove temporary data root ${DataRoot}: $($_.Exception.Message)"
                } else {
                    Start-Sleep -Milliseconds 300
                }
            }
        }
    }
}

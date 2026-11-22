param(
    [Parameter(Mandatory = $true)][string]$HostName,
    [int]$Port = 5432,
    [Parameter(Mandatory = $true)][string]$UserName,
    [string]$Owner = $UserName,
    [string]$DockerContainer,
    [ValidateSet("prefer", "require", "disable")][string]$SslMode = "prefer"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$catalogPath = Join-Path $repoRoot "services\python-query-service-corrected\app\data\sectors.json"
$catalog = Get-Content -LiteralPath $catalogPath -Raw | ConvertFrom-Json
$securePassword = Read-Host "PostgreSQL password" -AsSecureString
$passwordPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
$env:PGPASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPtr)
$env:PGSSLMODE = $SslMode

function Invoke-Psql([string[]]$PsqlArgs) {
    $baseArgs = @("-X", "-v", "ON_ERROR_STOP=1", "-U", $UserName)
    if ($DockerContainer) {
        $dockerArgs = @("exec", "-e", "PGPASSWORD=$env:PGPASSWORD", $DockerContainer, "psql") + $baseArgs + $PsqlArgs
        $output = & docker @dockerArgs
    } else {
        $psqlArgs = $baseArgs + @("-h", $HostName, "-p", [string]$Port) + $PsqlArgs
        $output = & psql @psqlArgs
    }
    if ($LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE)" }
    return $output
}

try {
    $created = 0
    $existing = 0
    foreach ($sector in $catalog.sectors) {
        $databaseName = [string]$sector.database_name
        if ($databaseName -notmatch '^searchengine_[a-z0-9_]{1,50}$') {
            throw "Unsafe database name in sectors.json: $databaseName"
        }
        $found = Invoke-Psql @("-d", "postgres", "-tAc", "SELECT 1 FROM pg_database WHERE datname='$databaseName'")
        if ($found -match '1') {
            $existing++
            continue
        }
        $quotedOwner = '"' + $Owner.Replace('"', '""') + '"'
        $createSql = "CREATE DATABASE `"$databaseName`" OWNER $quotedOwner"
        $null = Invoke-Psql @("-d", "postgres", "-c", $createSql)
        $created++
        Write-Host "Created $databaseName"
    }
    Write-Host "Done. Created $created databases; kept $existing existing databases."
    Write-Host "Rust will create the required tables and pgvector indexes lazily the first time each database is used."
}
finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:PGSSLMODE -ErrorAction SilentlyContinue
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPtr)
}

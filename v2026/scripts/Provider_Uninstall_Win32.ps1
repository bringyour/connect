#!/usr/bin/env pwsh

param(
    [Switch]$ForAllUsers = $false
);

$CurrentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())

if ($ForAllUsers) {
    if (-not $CurrentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Host "Not running as admin. Please run with elevated admin permissions."
        exit 1
    }
    else {
        Write-Host "Running as admin"
    }
}

$Arch = (Get-CimInstance Win32_Processor).Architecture
$Arch = switch ($Arch) {
    9 { "amd64" }
    12 { "arm64" }
    default { "unsupported" }
}

Write-Debug "Detected architecture: $Arch"

if ($Arch -eq "unsupported") {
    Write-Error "Unsupported architecture. Urnetwork only supports Windows on x64 and ARM64."
    exit 1
}

$DataDir = "$env:HOMEDRIVE$env:HOMEPATH\.urnetwork"
$InstallDir = Join-Path -Path $env:LOCALAPPDATA -ChildPath "urnetwork\provider"

if ($ForAllUsers) {
    $InstallDir = "C:\Program Files\urnetwork\provider"
}

if (-not (Test-Path $InstallDir)) {
    Write-Error "Urnetwork installation could not be detected on this PC."
    exit 1
}

$ProviderExe = Join-Path -Path $InstallDir -ChildPath "windows\$Arch\urnetwork.exe"
Get-WmiObject Win32_Process | Where-Object { $_.ExecutablePath -eq $ProviderExe } | ForEach-Object { $_.Terminate() }

Write-Host "Removing installation directory: $InstallDir"
Remove-Item -Path $InstallDir -Recurse -Force

if (!$?) {
    Write-Error "Could not remove the installation directory"
    exit 1
}

if (Test-Path $DataDir) {
    Write-Host "Removing data directory: $DataDir"
    Remove-Item -Path $DataDir -Recurse -Force

    if (!$?) {
        Write-Error "Could not remove the data directory"
        exit 1
    }
}

$StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"

if ($ForAllUsers) {
    $StartupPath = Join-Path -Path "C:\ProgramData" -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
}

$ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork.lnk"

if (Test-Path $ShortcutPath) {
    Write-Host "Removing startup entry: $ShortcutPath"
    Remove-Item -Path $ShortcutPath -Force

    if (!$?) {
        Write-Error "Could not remove the startup entry"
        exit 1
    }
}

function Get-Path {
    if ($ForAllUsers) {
        return [System.Environment]::GetEnvironmentVariable("PATH", [System.EnvironmentVariableTarget]::Machine)
    }

    return [Environment]::GetEnvironmentVariable("PATH", [System.EnvironmentVariableTarget]::User)
}

function Set-Path {
    param (
        [Parameter(Mandatory = $true)]
        [String]$Value
    )

    if ($ForAllUsers) {
        [System.Environment]::SetEnvironmentVariable("PATH", $Value, [System.EnvironmentVariableTarget]::Machine)
    }
    else {
        [Environment]::SetEnvironmentVariable("PATH", $Value, [System.EnvironmentVariableTarget]::User)
    }
}

$EnvValue = "$InstallDir\windows\$Arch"

$EnvPath = Get-Path
$EnvPathSplitted = $EnvPath.Split(";")

if ($EnvPathSplitted -contains $EnvValue) {
    Write-Host "Updating PATH variable"

    $NewPath = ($EnvPathSplitted | Where-Object { $_ -ne $EnvValue -and $_ -ne "" }) -join ';'
    Set-Path -Value $NewPath

    if (!$?) {
        Write-Error "Failed to update PATH variable. Please check your permissions."
        exit 1
    }
}

Write-Host "Uninstallation successful."
Write-Host "If Urnetwork provider is already running, you can terminate it from Task Manager. Otherwise, you can just restart your PC to stop it."

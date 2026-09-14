#!/usr/bin/env pwsh

param(
    [String]$Frequency,
    [String]$InstalledPath = ""
);

$Seconds = switch ($Frequency) {
    "every-day" { 60 * 60 * 24 }
    "every-week" { 60 * 60 * 24 * 7 }
    "every-month" { 60 * 60 * 24 * 30 }
    default {
	Write-Error "Invalid frequency: $Frequency"
	exit 1
    }
}

if (-not $InstalledPath) {
    $InstalledPath = Split-Path -Path $MyInvocation.MyCommand.Path
}

$ToolsPath = Join-Path $InstalledPath -ChildPath "urnet-tools.ps1"
$PIDFile = Join-Path $InstalledPath -ChildPath "urnetwork-updater.pid"

Set-Content $PIDFile "$PID"

while ($true) {
    Start-Sleep $Seconds
    & "$ToolsPath" update
}

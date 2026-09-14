#!/usr/bin/env pwsh
# Credits: Ar Rakin, Ryan Mello
# urnet-tools -- URnetwork manager script
# GitHub: <https://github.com/urnetwork/connect>
<#
.SYNOPSIS
    URnetwork manager and toolkit.

.DESCRIPTION
    This script helps you manage your URnetwork installation.

.PARAMETER Command
    The subcommand to execute. The first positional argument also
    corresponds to this.
    Subcommands:
    * update - Update URnetwork
    * uninstall - Uninstall URnetwork
    * reinstall - Reinstall URnetwork
    * status - Show provider status
    * start - Start the provider
    * stop - Stop the provider
    * version - Show the version of your URnetwork installation
    * auto-update-enable - Enable auto-updates
    * auto-update-freq - Change the frequency of auto-update checks
    * auto-update-disable - Disable auto-updates
    * auto-start-enable - Enable auto-start
    * auto-start-disable - Disable auto-start

.PARAMETER InstalledPath
    The path where URnetwork provider was installed. Defaults to %LOCALAPPDATA%\urnetwork\provider on Windows.

.PARAMETER Help
    Show this help and exit.

.EXAMPLE
    urnet-tools.ps1 update -InstalledPath "C:\Users\You\urnetwork"
    Runs operation "update" for the given installation path. 

.EXAMPLE
    urnet-tools.ps1 update
    Updates URnetwork.

.OUTPUTS
    String. Installer logs and messages.

.INPUTS
    None. Does not take any input.

.LINK
    https://docs.ur.io/provider
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet("uninstall", "update", "start", "stop", "status", "version", "reinstall", "auto-update-enable", "auto-update-disable", "auto-update-freq", "auto-start-enable", "auto-start-disable")]
    [String]$Command,
    [Switch]$Help = $false,
    [String]$InstalledPath = ""
);

if ($Help) {
    Get-Help $MyInvocation.MyCommand.Path -Full
    exit 0
}

if (-not $Command) {
    Write-Error "Please specify a command!"
    exit 1
}

if (-not $InstalledPath) {
    $InstalledPath = Split-Path -Path $MyInvocation.MyCommand.Path
}

$BinarySuffix = ""

if (-not $IsLinux) {
    $BinarySuffix = ".exe"
}

$GithubURLBase = "https://api.github.com/repos/urnetwork/connect"

function Get-Path {
    return [Environment]::GetEnvironmentVariable("PATH", [System.EnvironmentVariableTarget]::User)
}

function Set-Path {
    param (
        [Parameter(Mandatory = $true)]
        [String]$Value
    )

    [Environment]::SetEnvironmentVariable("PATH", $Value, [System.EnvironmentVariableTarget]::User)
}

$VersionPath = Join-Path $InstalledPath -ChildPath "version"
$InstallDatePath = Join-Path $InstalledPath -ChildPath "date"

function Check-Update {
    $ReleaseInfo = Invoke-RestMethod -Uri "$GithubURLBase/releases/latest"

    if (-not $ReleaseInfo) {
        Write-Error "Failed to fetch release information from GitHub API. Are you sure the version exists and your internet connection is working?"
        exit 1
    }

    $Tag = $ReleaseInfo.tag_name
    $PublishedAt = [DateTime]$ReleaseInfo.published_at
    $InstallDate = [DateTime](([String](Get-Content $InstallDatePath -Raw)).Trim())
    $InstalledTag = ([String](Get-Content $VersionPath -Raw)).Trim()

    if (-not $? -or -not $InstallDate) {
        Write-Error "Cannot read the installation date file at $InstallDatePath"
        exit 1
    }

    if ($InstallDate -ge $PublishedAt) {
        Write-Host "Installed version is up-to-date ($InstalledTag)"
        return $null
    }

    Write-Host "Update available ($Tag)"
    return $Tag
}

function Add-StartupCommand {
    param(
	    [String]$Frequency
    );
    
    $StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
    $ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork-update.lnk"

    if (Test-Path $ShortcutPath) {
	    Remove-Item -Path $ShortcutPath -Force
    }

    $Arguments = '-WindowStyle Hidden -File .\urnetwork-updater.ps1 -InstalledPath "' + $InstalledPath + '" -Frequency every-' + $Frequency
    
    $WshShell = New-Object -ComObject WScript.Shell
    $Shortcut = $WshShell.CreateShortcut($ShortcutPath)
    $Shortcut.TargetPath = "powershell.exe"
    $Shortcut.Arguments = $Arguments
    $Shortcut.WorkingDirectory = $InstalledPath
    $Shortcut.WindowStyle = 7
    $Shortcut.Save()

    return "powershell.exe $Arguments"
}

function Enable-AutoUpdate {
    param(
	    [String]$Frequency = "day"
    );
    
    $StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
    $ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork-update.lnk"

    if (Test-Path $ShortcutPath) {
        Write-Error "Auto update is already enabled!"
        exit 1
    }

    Add-StartupCommand -Frequency $Frequency
    Start-Process -FilePath $ShortcutPath -WindowStyle Hidden
    Write-Host "Auto update enabled (frequency: every $Frequency)"
}

function Disable-AutoUpdate {
    param(
	    [Switch]$NoError = $false
    );
    
    $StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
    $ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork-update.lnk"

    if (-not (Test-Path $ShortcutPath)) {
        if (!$NoError) {
            Write-Error "Auto update is already disabled!"
            exit 1
        }

        return
    }

    $PIDFile = Join-Path $InstalledPath -ChildPath "urnetwork-updater.pid"

    if (Test-Path $PIDFile) {
        $UpdaterPID = ([String](Get-Content -Raw $PIDFile)).Trim()

        if ($?) {
            Remove-Item $PIDFile -Force
            Stop-Process -Id $UpdaterPID -ErrorAction SilentlyContinue
        }
    }

    Remove-Item $ShortcutPath -Force
    Write-Host "Auto update disabled"
}

$BinaryPath = Join-Path $InstalledPath -ChildPath "urnetwork$BinarySuffix"

function Enable-AutoStart {
    $StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
	$ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork.lnk"

	if (Test-Path $ShortcutPath) {
	    Write-Error "Auto-start is already enabled"
        exit 1
	}

	$StartCommand = "Start-Process -FilePath '$BinaryPath' -ArgumentList 'provide' -WindowStyle Hidden"
	$Arguments = '-NoProfile -WindowStyle Hidden -Command "' + $StartCommand + '"'

	Write-Host "Startup command: powershell.exe $Arguments"

	$WshShell = New-Object -ComObject WScript.Shell
	$Shortcut = $WshShell.CreateShortcut($ShortcutPath)
	$Shortcut.TargetPath = "powershell.exe"
	$Shortcut.Arguments = $Arguments
	$Shortcut.WorkingDirectory = $Destination
	$Shortcut.WindowStyle = 7
	$Shortcut.Save()
}

function Disable-AutoStart {
    $StartupPath = Join-Path -Path $env:APPDATA -ChildPath "Microsoft\Windows\Start Menu\Programs\Startup"
    $ShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork.lnk"
    $UpdateShortcutPath = Join-Path -Path $StartupPath -ChildPath "urnetwork-update.lnk"

    if (Test-Path $ShortcutPath) {
	    Remove-Item -Path $ShortcutPath -Force
    }

    if (Test-Path $UpdateShortcutPath) {
	    Remove-Item -Path $UpdateShortcutPath -Force
    }
}

function Do-Uninstall {
    Write-Host "Uninstalling URnetwork provider"
    Disable-AutoUpdate -NoError
    
    $SelfPath = Join-Path $InstalledPath -ChildPath "urnet-tools.ps1"
    $UpdaterPath = Join-Path $InstalledPath -ChildPath "urnetwork-updater.ps1"

    Write-Host "Removing provider executable"
    
    if (Test-Path $BinaryPath) {
	    Remove-Item -Path $BinaryPath -Force
    }

    if (Test-Path $SelfPath) {
	    Remove-Item -Path $SelfPath -Force
    }

    if (Test-Path $UpdaterPath) {
	    Remove-Item -Path $UpdaterPath -Force
    }

    if (Test-Path $VersionPath) {
	    Remove-Item -Path $VersionPath -Force
    }

    if (Test-Path $InstallDatePath) {
	    Remove-Item -Path $InstallDatePath -Force
    }

    Write-Host "Removing data directory"
    
    $DataDir = ""

    if ($OS -eq "windows") {
	    $DataDir = "$env:HOMEDRIVE$env:HOMEPATH\.urnetwork"
    }
    else {
	    $DataDir = "$env:HOME/.urnetwork"
    }

    if (Test-Path $DataDir) {
	    Remove-Item -Path $DataDir -Recurse -Force
    }

    Write-Host "Removing startup entries (if any)"
    Disable-AutoStart
    
    $EnvValue = $InstalledPath
    $EnvPath = Get-Path
    $EnvPathSplitted = $EnvPath.Split(";")

    if ($EnvPathSplitted -contains $EnvValue) {
        Write-Host "Updating %PATH%"

        $NewPath = ($EnvPathSplitted | Where-Object { $_ -ne $EnvValue -and $_ -ne "" }) -join ';'
        Set-Path -Value $NewPath

        if (!$?) {
            Write-Error "Failed to update %PATH%"
            exit 1
        }
    }
}

function Get-URnetworkProcess {
    $Process = Get-Process | Where-Object { $_.ProcessName -eq 'urnetwork' }
    return $Process
}

switch ($Command) {
    "uninstall" {
        Do-Uninstall
        break	
    }

    "update" {
        $Tag = Check-Update

        if (-not $Tag) {
            Write-Host "No update is available"
            exit 0
        }
        
        Write-Host "Downloading the installer script of version $Tag"
        Write-Host "Executing installer script of version $Tag"

        $TempScriptPath = Join-Path $env:TEMP -ChildPath "urnetwork-installer.ps1"
        Invoke-RestMethod "https://raw.githubusercontent.com/urnetwork/connect/refs/heads/main/scripts/Provider_Install_Win32.ps1" -OutFile $TempScriptPath

        if (!$?) {
            Write-Error "Failed to download the installer script"
            exit 1
        }

        & $TempScriptPath -Destination $InstalledPath -NonInteractive

        if (-not $?) {
            Write-Error "Update failed"
            exit 1
        }

        Remove-Item $TempScriptPath -Force
        Write-Host "Update completed"
        break
    }

    "version" {
        $InstalledTag = ([String](Get-Content $VersionPath -Raw)).Trim()

        if (-not $? -or -not $InstalledTag) {
            Write-Error "Cannot read the version file at $VersionPath"
            exit 1
        }
        
        Write-Host "URnetwork version $InstalledTag"
        Check-Update
        break
    }

    "reinstall" {
        $InstalledTag = ([String](Get-Content $VersionPath -Raw)).Trim()

        if (-not $? -or -not $InstalledTag) {
            Write-Error "Cannot read the version file at $VersionPath"
            exit 1
        }	

        Do-Uninstall
        Write-Host "Downloading the installer script of version $InstalledTag"
        Write-Host "Executing installer script of version $InstallerTag"

        $TempScriptPath = Join-Path $env:TEMP -ChildPath "urnetwork-installer.ps1"
        Invoke-RestMethod "https://raw.githubusercontent.com/urnetwork/connect/refs/heads/main/scripts/Provider_Install_Win32.ps1" -OutFile $TempScriptPath

        if (!$?) {
            Write-Error "Failed to download the installer script"
            exit 1
        }

        & $TempScriptPath -Destination $InstalledPath -NonInteractive -Version $InstalledTag

        if (-not $?) {
            Write-Error "Reinstallation failed"
            exit 1
        }

        Remove-Item $TempScriptPath -Force
        Write-Host "Reinstallation completed"
        break
    }

    "auto-update-enable" {
        Enable-AutoUpdate
        break
    }

    "auto-update-disable" {
        Disable-AutoUpdate
        break
    }

    "auto-update-freq" {
        $Answer = Read-Host "How frequently do you want the update checks to happen? every-[day/week/month]"
        $Frequency = switch ($Answer) {
            "every-day" { "day" }
            "day" { "day" }
            "every-week" { "week" }
            "week" { "week" }
            "every-month" { "month" }
            "month" { "month" }
            default {
                Write-Error "Invalid frequency: $Frequency"
                exit 1
            }
        }
        
        Disable-AutoUpdate
        Enable-AutoUpdate -Frequency $Frequency 
        break
    }

    "auto-start-enable" {
        Enable-AutoStart
        break
    }

    "auto-start-disable" {
        Disable-AutoStart
        break
    }

    "start" {
        Start-Process -FilePath "$BinaryPath" -ArgumentList "provide" -WindowStyle Hidden
        break
    }

    "stop" {
        $Process = Get-URnetworkProcess
        
        if ($Process) {
            $URnetworkPID = $Process.Id
            Stop-Process -Id $URnetworkPID -ErrorAction SilentlyContinue
        }

        break
    }

    "status" {
        $Process = Get-URnetworkProcess 
        
        if ($Process) {
            Write-Host "Status: Running"
        }
        else {
            Write-Host "Status: Stopped"
        }

        break
    }

    default {
        Write-Error "Invalid command: $Command"
        exit 1
    }
}

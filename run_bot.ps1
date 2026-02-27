# run_bot.ps1 - SINGLE LAUNCHER TO RULE THEM ALL
param(
    [switch]$Install
)

$botRoot = "C:\Users\EndUser\Documents\GitHub\Multi-Strategy-Trade-Bot-v2"
$pythonPath = "C:\Users\EndUser\AppData\Local\Programs\Python\Python311\python.exe"
$logDir = "$botRoot\data\logs"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log {
    param($Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path "$logDir\launcher.log" -Value "$timestamp - $Message"
}

function Update-Code {
    Write-Log "Pulling latest code from GitHub"
    Set-Location $botRoot
    git pull
}

function Run-Python {
    param($Script)
    Write-Log "Running $Script"
    try {
        $process = Start-Process -FilePath $pythonPath -ArgumentList $Script -Wait -PassThru -NoNewWindow
        if ($process.ExitCode -eq 0) {
            Write-Log "$Script completed successfully"
        } else {
            Write-Log "$Script failed (exit: $($process.ExitCode))"
        }
    }
    catch {
        Write-Log "ERROR running $Script : $_"
    }
}

if ($Install) {
    Write-Host "Installing bot to run at startup..." -ForegroundColor Green
    $startup = [Environment]::GetFolderPath('Startup')
    $shortcut = Join-Path $startup "Mark3Bot.lnk"
    $wsh = New-Object -ComObject WScript.Shell
    $shortcut = $wsh.CreateShortcut($shortcut)
    $shortcut.TargetPath = "powershell.exe"
    $shortcut.Arguments = "-WindowStyle Hidden -ExecutionPolicy Bypass -File `"$botRoot\run_bot.ps1`""
    $shortcut.WorkingDirectory = $botRoot
    $shortcut.Save()
    Write-Host "Installation complete. Bot will start on next reboot." -ForegroundColor Green
    exit
}

Write-Log "=== Bot Launcher Started ==="
while ($true) {
    if ((Get-Date).Minute -eq 0) {
        Update-Code
    }
    
    $now = Get-Date
    $etNow = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId($now.ToUniversalTime(), "Eastern Standard Time")
    
    $isWeekday = $etNow.DayOfWeek -ge 'Monday' -and $etNow.DayOfWeek -le 'Friday'
    $marketOpen = [TimeSpan]::FromHours(9.5)
    $marketClose = [TimeSpan]::FromHours(16)
    $isMarketHours = $isWeekday -and $etNow.TimeOfDay -ge $marketOpen -and $etNow.TimeOfDay -le $marketClose
    
    if ($isMarketHours -and ($etNow.Minute % 5 -eq 0) -and $etNow.Second -lt 10) {
        Run-Python "scripts/build_candidates.py"
        Run-Python "scripts/main.py"
        Start-Sleep -Seconds 30
    }
    
    Start-Sleep -Seconds 1
}
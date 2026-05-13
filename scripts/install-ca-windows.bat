@echo off
:: RAYD CA Certificate Installer for Windows
:: Run as Administrator to trust RAYD HTTPS on this PC.
:: Double-click or: right-click → "Run as administrator"

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Please run this script as Administrator.
    echo Right-click the file and choose "Run as administrator".
    pause
    exit /b 1
)

set "SCRIPT_DIR=%~dp0"
set "CA_CERT=%SCRIPT_DIR%..\nginx\certs\rayd-ca.crt"

if not exist "%CA_CERT%" (
    echo ERROR: CA certificate not found at:
    echo   %CA_CERT%
    echo Make sure you are running this from the RAYD project folder.
    pause
    exit /b 1
)

echo Installing RAYD CA certificate into Trusted Root store...
certutil -addstore -f Root "%CA_CERT%"

if %errorlevel% equ 0 (
    echo.
    echo  Done! RAYD HTTPS is now trusted on this PC.
    echo  Restart Chrome or Edge if already open.
) else (
    echo.
    echo  ERROR: Installation failed. Check that you are running as Administrator.
)

pause

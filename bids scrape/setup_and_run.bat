@echo off
SetLocal EnableDelayedExpansion
echo ==========================================
echo    Gem Bids Scraper Setup & Run
echo ==========================================

REM Check for Python
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not found. Please install Python and add it to PATH.
    pause
    exit /b
)

echo [INFO] Installing requirements...
pip install -r requirements.txt
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install requirements.
    pause
    exit /b
)

REM Check for .env file
IF NOT EXIST .env (
    echo.
    echo [WARN] .env file not found! Let's configure your Database.
    echo.
    set /p DB_HOST="Enter Database Host (default: localhost): "
    IF "%DB_HOST%"=="" set DB_HOST=localhost
    
    set /p DB_PORT="Enter Database Port (default: 5432): "
    IF "%DB_PORT%"=="" set DB_PORT=5432
    
    set /p DB_NAME="Enter Database Name (default: bids_db): "
    IF "%DB_NAME%"=="" set DB_NAME=bids_db
    
    set /p DB_USER="Enter Database User (default: postgres): "
    IF "%DB_USER%"=="" set DB_USER=postgres
    
    set /p DB_PASSWORD="Enter Database Password: "
    
    echo Creating .env file...
    (
        echo DB_HOST=!DB_HOST!
        echo DB_PORT=!DB_PORT!
        echo DB_NAME=!DB_NAME!
        echo DB_USER=!DB_USER!
        echo DB_PASSWORD=!DB_PASSWORD!
    ) > .env
    
    echo [INFO] .env file created successfully.
)

echo [INFO] Starting Scraper in TEST mode (10 pages)...
echo [INFO] Ensure PostgreSQL is running and .env is configured if needed.
echo.

python scrape_bids.py -t

echo.
echo [INFO] Done.
pause

@echo off
REM Builds everything needed to run GeM Scraper on any Windows 10/11 PC (no Python/Node needed):
REM   dist\installer\GeM Scraper Setup.exe   - installer (shortcuts, uninstaller)   <- give this to users
REM   dist\portable\GeM Scraper.exe          - single-file portable app, runs from anywhere
REM   dist\GeM Scraper\                      - the app folder the installer packs
cd /d "%~dp0"

if not exist venv\Scripts\python.exe (
    echo Creating Python environment...
    py -3.11 -m venv venv || python -m venv venv
)
venv\Scripts\python.exe -m pip install -q -r requirements.txt pyinstaller==6.11.1 || goto :error

set EXCLUDES=--exclude-module pandas --exclude-module numpy --exclude-module matplotlib --exclude-module scipy --exclude-module IPython --exclude-module pytest --exclude-module tqdm --exclude-module PyPDF2 --exclude-module pyflakes

echo === 1/3 app folder ===
venv\Scripts\python.exe -m PyInstaller --noconfirm --clean --windowed --onedir --name "GeM Scraper" ^
  --icon assets\gem_scraper.ico %EXCLUDES% gem_scraper_app.py || goto :error

echo === 2/3 portable single-file exe ===
venv\Scripts\python.exe -m PyInstaller --noconfirm --windowed --onefile --name "GeM Scraper" ^
  --icon assets\gem_scraper.ico --distpath dist\portable --workpath build\portable %EXCLUDES% ^
  gem_scraper_app.py || goto :error

echo === 3/3 installer ===
set ISCC=%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe
if not exist "%ISCC%" set ISCC=%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe
if not exist "%ISCC%" (
    echo Inno Setup not found - install it with:  winget install JRSoftware.InnoSetup
    goto :error
)
"%ISCC%" /Q installer\gem_scraper.iss || goto :error

echo.
echo Done:
echo   dist\installer\GeM Scraper Setup.exe
echo   dist\portable\GeM Scraper.exe
exit /b 0

:error
echo Build failed.
exit /b 1

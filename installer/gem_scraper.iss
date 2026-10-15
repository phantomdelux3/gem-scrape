; Inno Setup script for GeM Contract Scraper.
; Built by build_exe.bat -> dist\installer\GeM Scraper Setup.exe
; Self-contained: Python and all libraries are bundled, nothing else needs to be installed.

#define AppName "GeM Scraper"
#define AppVersion "3.0"
#define AppExe "GeM Scraper.exe"

[Setup]
AppId={{6F1C2B7A-3E54-4C8B-9B0E-5A1D7C2E9F41}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher=GeM Scraper
; Installed to a normal, writable folder so PDFs + the database can live next to the app.
DefaultDirName={sd}\GeM Scraper
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=6.3
OutputDir=..\dist\installer
OutputBaseFilename=GeM Scraper Setup
SetupIconFile=..\assets\gem_scraper.ico
UninstallDisplayIcon={app}\{#AppExe}
Compression=lzma2/max
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Shortcuts:"

[Files]
; the complete app folder: the .exe plus its _internal folder (Python runtime + libraries)
Source: "..\dist\GeM Scraper\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
; data folder (PDFs, gem_data.db, exports) - kept when the app is uninstalled
Name: "{app}\GeM Data"; Flags: uninsneveruninstall

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExe}"; WorkingDir: "{app}"
Name: "{group}\GeM Data folder"; Filename: "{app}\GeM Data"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExe}"; Description: "Start {#AppName} now"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; remove only files the app creates itself, never the user's data
Type: filesandordirs; Name: "{app}\_internal"

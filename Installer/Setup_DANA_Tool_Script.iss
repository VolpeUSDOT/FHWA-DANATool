; Script generated by the Inno Script Studio Wizard.
; SEE THE DOCUMENTATION FOR DETAILS ON CREATING INNO SETUP SCRIPT FILES!

#define MyAppName "FHWA DANA Tool"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Federal Higwhay Administration"
#define MyAppURL "https://www.fhwa.dot.gov/environment/"
#define MyAppExeName "DANATool.exe"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
; (To generate a new GUID, click Tools | Generate GUID inside the IDE.)
AppId={{A8E58A55-0E3E-4799-BF3B-51BE18E7D180}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
;AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={localappdata}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=D:\DANA\Installer
OutputBaseFilename=Setup_DANA_Tool
SetupIconFile=D:\DANA\Installer\icon.ico
Compression=lzma
SolidCompression=yes
PrivilegesRequired=none

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "D:\DANA\Installer\Final V1.0\DANATool.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "D:\DANA\Installer\Final V1.0\NTD_05_main_GUI.py"; DestDir: "{app}"; Flags: ignoreversion
Source: "D:\DANA\Installer\Final V1.0\lib\*"; DestDir: "{app}\lib"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "D:\DANA\Installer\Final V1.0\User Input Files\*"; DestDir: "{app}\User Input Files"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "D:\DANA\Installer\Final V1.0\Default Input Files\FIPS_County_Codes.csv"; DestDir: "{app}\Default Input Files"; Flags: ignoreversion
Source: "D:\DANA\Installer\Final V1.0\Default Input Files\NEI2017_RepresentativeCounties.csv"; DestDir: "{app}\Default Input Files"; Flags: ignoreversion
Source: "D:\DANA\Installer\Final V1.0\Default Input Files\NEI2017_RepresentativeEmissionsRates.csv"; DestDir: "{app}\Default Input Files"; Flags: ignoreversion
Source: "D:\DANA\Installer\Final V1.0\Default Input Files\HPMS County Road Mileage\*"; DestDir: "{app}\Default Input Files\HPMS County Road Mileage"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "D:\DANA\Installer\Final V1.0\Default Input Files\Statewide Functional Class VMT\*"; DestDir: "{app}\Default Input Files\Statewide Functional Class VMT"; Flags: ignoreversion recursesubdirs createallsubdirs
; NOTE: Don't use "Flags: ignoreversion" on any shared system files

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:ProgramOnTheWeb,{#MyAppName}}"; Filename: "{#MyAppURL}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent
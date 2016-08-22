@echo off
set ZIPFILE=D:\ownCloud\tce\tceRelay.zip
set BUILDDIR=build\exe.win-amd64-3.4

REM Clean up build area
del /s /q "%BUILDDIR%"

python setup.py build

REM Copy DB
copy TCE-RelayClient.db "%BUILDDIR%"

REM Del ZIP
del "%ZIPFILE%"

REM Create ZIP
powershell.exe -nologo -noprofile -command "& { Add-Type -A 'System.IO.Compression.FileSystem'; [IO.Compression.ZipFile]::CreateFromDirectory('%BUILDDIR%', '%ZIPFILE%'); }"

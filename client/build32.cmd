@echo off
set ZIPFILE=D:\ownCloud\tce\tceRelayClient32.zip
set BUILDDIR=build\exe.win32-3.4
set COPYTO=C:\TCE\TCE-RelayClient
set PYTHONEXE32=C:\Python34-32\python.exe

REM Clean up build area
del /s /q "%BUILDDIR%"

REM Build
%PYTHONEXE32% setup.py build

REM Copy DB
copy TCE-RelayClient.db "%BUILDDIR%"

REM Copy License
copy ..\LICENSE.TXT "%BUILDDIR%"

REM Del ZIP
del "%ZIPFILE%"

REM Copy files
xcopy /I /Y "%BUILDDIR%" "%COPYTO%"

REM Create ZIP
powershell.exe -nologo -noprofile -command "& { Add-Type -A 'System.IO.Compression.FileSystem'; [IO.Compression.ZipFile]::CreateFromDirectory('%BUILDDIR%', '%ZIPFILE%'); }"

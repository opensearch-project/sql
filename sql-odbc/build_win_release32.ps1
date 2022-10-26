$WORKING_DIR = (Get-Location).Path
$env:VCPKG_DEFAULT_TRIPLET = 'x86-windows'
cd src
vcpkg install
cd ..
.\scripts\build_windows.ps1 $WORKING_DIR Release 32

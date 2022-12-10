# Build AWS SDK
$CURRENT_DIR = Get-Location
$WORKING_DIR = $args[0]
$CONFIGURATION = $args[1]
$BITNESS = $args[2]
if ($BITNESS -eq "64") {
    $WIN_ARCH = "x64"
}
else {
    $WIN_ARCH = "Win32"
}

# Create build directory; remove if exists
$BUILD_DIR = "${WORKING_DIR}\build"
New-Item -Path $BUILD_DIR -ItemType Directory -Force | Out-Null

Set-Location $CURRENT_DIR

# Build driver
$DRIVER_SOURCE_DIR = "${WORKING_DIR}\src"
$DRIVER_BUILD_DIR = "${BUILD_DIR}\odbc\cmake"
$VCPKG_INSTALLED_DIR = "${DRIVER_SOURCE_DIR}\vcpkg_installed\$env:VCPKG_DEFAULT_TRIPLET"

.\scripts\build_driver.ps1 `
    $CONFIGURATION $WIN_ARCH `
    $DRIVER_SOURCE_DIR $DRIVER_BUILD_DIR $VCPKG_INSTALLED_DIR
Set-Location $CURRENT_DIR

# Move driver dependencies to bin directory for testing
$DRIVER_BIN_DIR = "${BUILD_DIR}\odbc\bin\$CONFIGURATION"
New-Item -Path $DRIVER_BIN_DIR -ItemType Directory -Force | Out-Null

Copy-Item $VCPKG_INSTALLED_DIR\bin\* $DRIVER_BIN_DIR
if ($BITNESS -eq "32") {
    # Strip bitness from 32bit VLD DLL dir name
    $BITNESS = $null
    $WIN_ARCH = "x86"
}
Copy-Item .\libraries\VisualLeakDetector\bin$BITNESS\vld_$WIN_ARCH.dll $DRIVER_BIN_DIR

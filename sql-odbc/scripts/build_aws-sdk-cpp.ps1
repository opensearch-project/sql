$CONFIGURATION = $args[0]
$WIN_ARCH = $args[1]
$SRC_DIR = $args[2]
$BUILD_DIR = $args[3]
$INSTALL_DIR = $args[4]
$VCPKG_DIR = $args[5]
$LIBCURL_WIN_ARCH = $args[6]

Write-Host $args

# Clone the AWS SDK CPP repo
$SDK_VER = "1.9.199"

git clone `
    --branch `
    $SDK_VER `
    --single-branch `
    "https://github.com/aws/aws-sdk-cpp.git" `
    --recurse-submodules `
    $SRC_DIR

# Make and move to build directory
New-Item -Path $BUILD_DIR -ItemType Directory -Force | Out-Null
Set-Location $BUILD_DIR

# Configure and build 
cmake $SRC_DIR `
    -A $WIN_ARCH `
    -D CMAKE_VERBOSE_MAKEFILE=ON `
    -D CMAKE_INSTALL_PREFIX=$INSTALL_DIR `
    -D CMAKE_BUILD_TYPE=$CONFIGURATION `
    -D BUILD_ONLY="core" `
    -D ENABLE_UNITY_BUILD="ON" `
    -D CUSTOM_MEMORY_MANAGEMENT="OFF" `
    -D ENABLE_RTTI="OFF" `
    -D ENABLE_TESTING="OFF" `
    -D FORCE_CURL="ON" `
    -D ENABLE_CURL_CLIENT="ON" `
    -DCMAKE_TOOLCHAIN_FILE="${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake" `
    -D CURL_LIBRARY="${VCPKG_DIR}/packages/curl_${LIBCURL_WIN_ARCH}-windows/lib" `
    -D CURL_INCLUDE_DIR="${VCPKG_DIR}/packages/curl_${LIBCURL_WIN_ARCH}-windows/include/"

# Build AWS SDK and install to $INSTALL_DIR 
msbuild ALL_BUILD.vcxproj /m /p:Configuration=$CONFIGURATION
msbuild INSTALL.vcxproj /m /p:Configuration=$CONFIGURATION

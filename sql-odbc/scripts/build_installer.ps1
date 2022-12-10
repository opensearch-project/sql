$CONFIGURATION = $args[0]
$WIN_ARCH = $args[1]
$SRC_DIR = $args[2]
$BUILD_DIR = $args[3]
$INSTALL_DIR = $args[4]

Write-Host $args

# aws-sdk-cpp fails compilation with warning:
# "Various members of std::allocator are deprecated in C++17"
$env:CL='-D_SILENCE_CXX17_OLD_ALLOCATOR_MEMBERS_DEPRECATION_WARNING'

cmake -S $SRC_DIR `
    -B $BUILD_DIR `
    -A $WIN_ARCH `
    -D CMAKE_BUILD_TYPE=$CONFIGURATION `
    -D CMAKE_INSTALL_PREFIX=$INSTALL_DIR `
    -D BUILD_WITH_TESTS=OFF

# # Build Project
msbuild $BUILD_DIR\PACKAGE.vcxproj -m -p:Configuration=Release

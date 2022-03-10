$SRC_DIR = $args[0]
$LIBCURL_WIN_ARCH = $args[1]

if (!("${SRC_DIR}/packages/curl_${LIBCURL_WIN_ARCH}-windows" | Test-Path))
{
	git clone https://github.com/Microsoft/vcpkg.git $SRC_DIR
	Set-Location $SRC_DIR
	cmd.exe /c bootstrap-vcpkg.bat
	.\vcpkg.exe integrate install
	.\vcpkg.exe install curl[tool]:${LIBCURL_WIN_ARCH}-windows
}

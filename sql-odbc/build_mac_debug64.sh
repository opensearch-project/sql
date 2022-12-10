#!/bin/bash

cd src
vcpkg install
cd ..

vcpkg_installed_dir='x64-osx'
if [[ $MACHTYPE == 'arm64-apple-darwin'* ]]; then
  vcpkg_installed_dir='arm64-osx'
fi

PREFIX_PATH=$(pwd)
mkdir cmake-build64
cd cmake-build64
cmake ../src -DCMAKE_INSTALL_PREFIX=${PREFIX_PATH}/src/vcpkg_installed/${vcpkg_installed_dir}/ -DCMAKE_BUILD_TYPE=Debug -DBUILD_ONLY="core" -DCUSTOM_MEMORY_MANAGEMENT="OFF" -DENABLE_RTTI="OFF" -DENABLE_TESTING="OFF"
cd ..

cmake --build cmake-build64 -- -j 4

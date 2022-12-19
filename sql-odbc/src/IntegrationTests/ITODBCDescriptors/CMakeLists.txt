project(itodbc_descriptors)

# Source, headers, and include dirs
set(SOURCE_FILES test_odbc_descriptors.cpp)
include_directories(	${UT_HELPER}
						${IT_HELPER}
						${OPENSEARCHODBC_SRC}
						${VLD_SRC}  )

# Generate executable
add_executable(itodbc_descriptors ${SOURCE_FILES})

# Find packages from vcpkg
find_package(GTest CONFIG REQUIRED)

# Library dependencies
target_code_coverage(itodbc_descriptors PUBLIC AUTO ALL)
target_link_libraries(itodbc_descriptors sqlodbc itodbc_helper ut_helper GTest::gtest_main)
target_compile_definitions(itodbc_descriptors PUBLIC _UNICODE UNICODE)

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

project(ut_critical_section)

# Source, headers, and include dirs
set(SOURCE_FILES test_critical_section.cpp)
include_directories(	${UT_HELPER}
						${OPENSEARCHODBC_SRC}
						${RAPIDJSON_SRC}
						${VLD_SRC}
						${RABBIT_SRC}  )

# Generate executable
add_executable(ut_critical_section ${SOURCE_FILES})

# Library dependencies
target_link_libraries(ut_critical_section sqlodbc ut_helper gtest_main)
target_compile_definitions(ut_critical_section PUBLIC _UNICODE UNICODE)

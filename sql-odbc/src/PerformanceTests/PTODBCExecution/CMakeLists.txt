project(performance_execution)

# Source, headers, and include dirs
set(SOURCE_FILES performance_odbc_execution.cpp)
include_directories(${OPENSEARCHODBC_SRC})

# Generate executable
add_executable(performance_execution ${SOURCE_FILES})

# Library dependencies
target_link_libraries(performance_execution sqlodbc)
target_compile_definitions(performance_execution PUBLIC _UNICODE UNICODE)

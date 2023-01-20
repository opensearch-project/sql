include(CMakeFindDependencyMacro)
find_dependency(aws-c-common)
find_dependency(aws-checksums)

if (BUILD_SHARED_LIBS)
    include(${CMAKE_CURRENT_LIST_DIR}/shared/aws-c-event-stream-targets.cmake)
else()
    include(${CMAKE_CURRENT_LIST_DIR}/static/aws-c-event-stream-targets.cmake)
endif()


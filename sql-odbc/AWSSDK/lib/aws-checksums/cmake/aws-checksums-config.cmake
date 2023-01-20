if (BUILD_SHARED_LIBS)
    include(${CMAKE_CURRENT_LIST_DIR}/shared/aws-checksums-targets.cmake)
else()
    include(${CMAKE_CURRENT_LIST_DIR}/static/aws-checksums-targets.cmake)
endif()


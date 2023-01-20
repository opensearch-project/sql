#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "AWS::aws-c-event-stream" for configuration "Release"
set_property(TARGET AWS::aws-c-event-stream APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(AWS::aws-c-event-stream PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libaws-c-event-stream.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS AWS::aws-c-event-stream )
list(APPEND _IMPORT_CHECK_FILES_FOR_AWS::aws-c-event-stream "${_IMPORT_PREFIX}/lib/libaws-c-event-stream.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)

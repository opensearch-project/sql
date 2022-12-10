project(sqlodbc)

# Source files for sqlodbc
set(					SOURCE_FILES 
							bind.c					columninfo.c			connection.c 				convert.c
							descriptor.c			dlg_specific.c			drvconn.c					options.c	
							environ.c				execute.c				info.c						loadlib.c
							misc.c					multibyte.c				mylog.c						tuple.c
							parse.c					results.c				statement.c					odbcapi30.c	
							qresult.c				odbcapi30w.c opensearch_api30.c opensearch_types.c
		opensearch_utility.cpp opensearch_communication.cpp opensearch_connection.cpp opensearch_odbc.c
        opensearch_driver_connect.cpp opensearch_helper.cpp opensearch_info.cpp opensearch_parse_result.cpp
		opensearch_semaphore.cpp opensearch_statement.cpp win_unicode.c				odbcapi.c
							odbcapiw.c opensearch_result_queue.cpp
	)
if(WIN32)
set(SOURCE_FILES ${SOURCE_FILES} dlg_wingui.c setup.c)
endif()

set(					HEADER_FILES 
							bind.h					catfunc.h				columninfo.h
							convert.h				descriptor.h			dlg_specific.h		drvconn.h
							environ.h opensearch_apifunc.h opensearch_communication.h opensearch_parse_result.h
        opensearch_connection.h opensearch_driver_connect.h opensearch_helper.h opensearch_info.h
        opensearch_statement.h opensearch_types.h loadlib.h
							misc.h					multibyte.h				mylog.h opensearch_utility.h
							resource.h				statement.h				tuple.h				unicode_support.h
		opensearch_apifunc.h opensearch_odbc.h opensearch_semaphore.h qresult.h
							version.h				win_setup.h opensearch_result_queue.h
	)

# Generate dll (SHARED)
if(WIN32)
set(RESOURCE_FILES opensearch_odbc.rc)
add_library(sqlodbc SHARED ${SOURCE_FILES} ${HEADER_FILES} ${RESOURCE_FILES} ${AWSSDK_LIB_DIR})
else()
add_library(sqlodbc SHARED ${SOURCE_FILES} ${HEADER_FILES})
endif()

include_directories(
						${LIBRARY_DIRECTORY}/../src
						${CMAKE_CURRENT_SOURCE_DIR}
						${OPENSEARCHENLIST_SRC}
						${RABBIT_SRC}
						${RAPIDJSON_SRC}
						${AWSSDK_INCLUDE_DIR}
					)

# Platform specific library dependencies
if(WIN32)
	# Windows specifiec
	target_link_libraries(sqlodbc wsock32 ws2_32 winmm user32 gdi32 legacy_stdio_definitions aws-cpp-sdk-core kernel32 advapi32 secur32 XOleHlp Wldap32 crypt32 Normaliz odbccp32 odbc32)
	target_link_libraries(sqlodbc debug msvcrtd)
	target_link_libraries(sqlodbc optimized msvcrt)
elseif(APPLE)
	# Apple specific
	target_link_libraries(sqlodbc iodbc iodbcinst aws-cpp-sdk-core)
elseif(UNIX)
	# Unix specific
	include_directories(/usr/src/linux-headers-5.0.0-27/include)
	target_link_libraries(sqlodbc aws-cpp-sdk-core odbc odbcinst)
endif()

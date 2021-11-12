/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef __OPENSEARCH_HELPER_H__
#define __OPENSEARCH_HELPER_H__

#include "opensearch_types.h"

#ifdef __cplusplus
// C++ interface
std::string OpenSearchGetClientEncoding(void* opensearch_conn);
bool OpenSearchSetClientEncoding(void* opensearch_conn, std::string& encoding);
OpenSearchResult* OpenSearchGetResult(void* opensearch_conn);
void OpenSearchClearResult(OpenSearchResult* opensearch_result);
void* OpenSearchConnectDBParams(runtime_options& rt_opts, int expand_dbname,
                        unsigned int option_count);
std::string GetServerVersion(void* opensearch_conn);
std::string GetClusterName(void* opensearch_conn);
std::string GetErrorMsg(void* opensearch_conn);
ConnErrorType GetErrorType(void* opensearch_conn);
std::vector< std::string > OpenSearchGetColumnsWithSelectQuery(
    void* opensearch_conn, const std::string table_name);

// C Interface
extern "C" {
#endif
void XPlatformInitializeCriticalSection(void** critical_section_helper);
void XPlatformEnterCriticalSection(void* critical_section_helper);
void XPlatformLeaveCriticalSection(void* critical_section_helper);
void XPlatformDeleteCriticalSection(void** critical_section_helper);
ConnStatusType OpenSearchStatus(void* opensearch_conn);
int OpenSearchExecDirect(void* opensearch_conn, const char* statement, const char* fetch_size);
void OpenSearchSendCursorQueries(void* opensearch_conn, const char* cursor);
void OpenSearchDisconnect(void* opensearch_conn);
void OpenSearchStopRetrieval(void* opensearch_conn);
#ifdef __cplusplus
}
#endif

void* InitializeOpenSearchConn();

#endif  // __OPENSEARCH_HELPER_H__

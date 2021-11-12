/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef _OPENSEARCH_PARSE_RESULT_H_
#define _OPENSEARCH_PARSE_RESULT_H_
#include "qresult.h"

#ifdef __cplusplus
std::string GetResultParserError();
extern "C" {
#endif
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#include "opensearch_helper.h"
typedef rabbit::document json_doc;
// const char* is used instead of string for the cursor, because a NULL cursor
// is sometimes used Cannot pass q_res as reference because it breaks qresult.h
// macros that expect to use -> operator
BOOL CC_from_OpenSearchResult(QResultClass *q_res, ConnectionClass *conn,
                      const char *cursor,
                              OpenSearchResult &opensearch_result);
BOOL CC_Metadata_from_OpenSearchResult(QResultClass *q_res, ConnectionClass *conn,
                               const char *cursor,
                                       OpenSearchResult &opensearch_result);
BOOL CC_No_Metadata_from_OpenSearchResult(QResultClass *q_res, ConnectionClass *conn,
                                  const char *cursor,
                                          OpenSearchResult &opensearch_result);
BOOL CC_Append_Table_Data(json_doc &opensearch_result_doc, QResultClass *q_res,
                          size_t doc_schema_size, ColumnInfoClass &fields);
#endif
#endif

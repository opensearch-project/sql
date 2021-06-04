/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

#ifndef __OPENSEARCH_DRIVER_CONNECT_H__
#define __OPENSEARCH_DRIVER_CONNECT_H__
#include "opensearch_connection.h"

// C Interface
#ifdef __cplusplus
extern "C" {
#endif
RETCODE OPENSEARCHAPI_DriverConnect(HDBC hdbc, HWND hwnd, SQLCHAR *conn_str_in,
                            SQLSMALLINT conn_str_in_len, SQLCHAR *conn_str_out,
                            SQLSMALLINT conn_str_out_len,
                            SQLSMALLINT *pcb_conn_str_out,
                            SQLUSMALLINT driver_completion);
#ifdef __cplusplus
}
#endif

#endif /* __OPENSEARCH_DRIVER_CONNECT_H__ */

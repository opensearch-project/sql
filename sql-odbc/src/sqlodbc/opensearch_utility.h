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

#ifndef OPENSEARCH_UTILITY_H
#define OPENSEARCH_UTILITY_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OpenSearchExpBufferData {
    char *data;
    size_t len;
    size_t maxlen;
} OpenSearchExpBufferData;

typedef OpenSearchExpBufferData *OpenSearchExpBuffer;

#define OpenSearchExpBufferBroken(str) ((str) == NULL || (str)->maxlen == 0)
#define OpenSearchExpBufferDataBroken(buf) ((buf).maxlen == 0)
#define INITIAL_EXPBUFFER_SIZE 256

void InitOpenSearchExpBuffer(OpenSearchExpBuffer str);
void AppendOpenSearchExpBuffer(OpenSearchExpBuffer str, const char *fmt, ...);
void TermOpenSearchExpBuffer(OpenSearchExpBuffer str);

#ifdef __cplusplus
}
#endif

#endif /* OPENSEARCH_UTILITY_H */

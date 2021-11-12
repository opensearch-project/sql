/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

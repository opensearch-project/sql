/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OPENSEARCH_RESULT_QUEUE
#define OPENSEARCH_RESULT_QUEUE

#include <mutex>
#include <queue>

#include "opensearch_semaphore.h"

#define QUEUE_TIMEOUT 20 // milliseconds

struct OpenSearchResult;

class OpenSearchResultQueue {
    public:
        OpenSearchResultQueue(unsigned int capacity);
        ~OpenSearchResultQueue();

        void clear();
        bool pop(unsigned int timeout_ms, OpenSearchResult*& result);
        bool push(unsigned int timeout_ms, OpenSearchResult* result);

    private:
        std::queue< OpenSearchResult*> m_queue;
        std::mutex m_queue_mutex;
        opensearch_semaphore m_push_semaphore;
        opensearch_semaphore m_pop_semaphore;
};

#endif

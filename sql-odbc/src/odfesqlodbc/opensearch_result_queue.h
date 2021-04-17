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

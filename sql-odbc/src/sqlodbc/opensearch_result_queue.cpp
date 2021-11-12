/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

#include "opensearch_result_queue.h"

#include "opensearch_types.h"

OpenSearchResultQueue::OpenSearchResultQueue(unsigned int capacity)
    : m_push_semaphore(capacity, capacity),
      m_pop_semaphore(0, capacity) {
}

OpenSearchResultQueue::~OpenSearchResultQueue() {
    while (!m_queue.empty()) {
        delete m_queue.front();
        m_queue.pop();
    }
}

void OpenSearchResultQueue::clear() {
    std::scoped_lock lock(m_queue_mutex);
    while (!m_queue.empty()) {
        delete m_queue.front();
        m_queue.pop();
        m_push_semaphore.release();
        m_pop_semaphore.lock();
    }
}

bool OpenSearchResultQueue::pop(unsigned int timeout_ms, OpenSearchResult*& result) {
    if (m_pop_semaphore.try_lock_for(timeout_ms)) {
        std::scoped_lock lock(m_queue_mutex);
        result = m_queue.front();
        m_queue.pop();
        m_push_semaphore.release();
        return true;
    }

    return false;
}

bool OpenSearchResultQueue::push(unsigned int timeout_ms, OpenSearchResult* result) {
    if (m_push_semaphore.try_lock_for(timeout_ms)) {
        std::scoped_lock lock(m_queue_mutex);
        m_queue.push(result);
        m_pop_semaphore.release();
        return true;
    }

    return false;
}

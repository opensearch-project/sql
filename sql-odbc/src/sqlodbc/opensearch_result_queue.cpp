/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

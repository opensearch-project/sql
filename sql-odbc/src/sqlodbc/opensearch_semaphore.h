/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OPENSEARCH_SEMAPHORE
#define OPENSEARCH_SEMAPHORE

#ifdef WIN32
  #include <windows.h>
#elif defined(__APPLE__)
  #include <dispatch/dispatch.h>
#else 
  #include <semaphore.h>
#endif

class opensearch_semaphore {
    public:
     opensearch_semaphore(unsigned int initial, unsigned int capacity);
        ~opensearch_semaphore();

        void lock();
        void release();
        bool try_lock_for(unsigned int timeout_ms);

    private:
#ifdef WIN32
        HANDLE m_semaphore;
#elif defined(__APPLE__)
        dispatch_semaphore_t m_semaphore;
#else 
        sem_t m_semaphore;
#endif
};

#endif

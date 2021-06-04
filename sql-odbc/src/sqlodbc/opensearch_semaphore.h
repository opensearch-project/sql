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

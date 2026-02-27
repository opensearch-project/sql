/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;


import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


public class SQLQueryTask extends CancellableTask {

    private final AtomicReference<Thread> executionThread = new AtomicReference<>();

    public SQLQueryTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    public void setExecutionThread(Thread thread) {
        executionThread.set(thread);
    }

    public void clearExecutionThread() {
        executionThread.set(null);
    }

    @Override
    public void onCancelled() {
        Thread thread = executionThread.get();
        if (thread != null) {
            thread.interrupt();
        }
    }
}

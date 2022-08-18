package org.opensearch;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.support.AbstractClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

public class NoOpClient extends AbstractClient {
    public NoOpClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool);
    }

    public NoOpClient(String testName) {
        super(Settings.EMPTY, null);
    }

    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        listener.onResponse(null);
    }

    public void close() {
        try {
            ThreadPool.terminate(this.threadPool(), 10L, TimeUnit.SECONDS);
        } catch (Exception var2) {
            throw new OpenSearchException(var2.getMessage(), var2, new Object[0]);
        }
    }
}

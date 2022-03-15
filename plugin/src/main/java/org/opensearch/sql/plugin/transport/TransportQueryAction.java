package org.opensearch.sql.plugin.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.commons.sql.action.SQLActions;
import org.opensearch.commons.sql.action.TransportQueryRequest;
import org.opensearch.commons.sql.action.TransportQueryResponse;
import org.opensearch.commons.sql.model.QueryType;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.commons.utils.TransportHelpersKt;

import java.io.IOException;


public class TransportQueryAction extends HandledTransportAction<ActionRequest, TransportQueryResponse> {
    private final Client client;
    private final Logger logger = LogManager.getLogger(TransportQueryAction.class);

    @Inject
    public TransportQueryAction(TransportService transpoertService, ActionFilters actionFilters, Client client) {
        super(SQLActions.SEND_SQL_QUERY_NAME, transportService, actionFilters, TransportQueryRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<TransportQueryResponse> listener) {
        logger.info("zhongnan su result");
        ThreadContext.StoredContext storedThreadContext = client.threadPool().getThreadContext().newStoredContext(false);

        TransportQueryRequest transformedRequest;
        if (request instanceof TransportQueryRequest) {
            transformedRequest = (TransportQueryRequest) request;
        } else {
            transformedRequest = TransportHelpersKt.recreateObject(request, streamInput -> {
                try {
                    return new TransportQueryRequest(streamInput);
                } catch (IOException e) {
                    logger.info("zhongnan exception in sql do execute");
                    listener.onFailure(e);
                }
                return null;
            }
            );
        }
        logger.info("zhongnan recreateObject done");
        if (transformedRequest.getType() == QueryType.PPL) {
            PPLQueryHelper.execute(transformedRequest, listener);
        } else if (transformedRequest.getType() == QueryType.SQL) {
            SQLQueryHelper.execute(transformedRequest, listener);
        }
    }

    private User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        return User.parse(userStr);
    }
}
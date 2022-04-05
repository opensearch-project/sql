/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.transport;

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

/**
 * Send SQL/PPL query transport action
 */
public class TransportQueryAction extends HandledTransportAction<ActionRequest, TransportQueryResponse> {
    private final Client client;

    @Inject
    public TransportQueryAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(SQLActions.SEND_SQL_QUERY_NAME, transportService, actionFilters, TransportQueryRequest::new);
        this.client = client;
    }

    /**
     * {@inheritDoc}
     * Transform the request and call super.doExecute() to support call from other plugins.
     */
    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<TransportQueryResponse> listener) {
        TransportQueryRequest transformedRequest;
        if (request instanceof TransportQueryRequest) {
            transformedRequest = (TransportQueryRequest) request;
        } else {
            transformedRequest = TransportHelpersKt.recreateObject(request, streamInput -> {
                try {
                    return new TransportQueryRequest(streamInput);
                } catch (IOException e) {
                    listener.onFailure(e);
                }
                return null;
            }
            );
        }
        if (transformedRequest.getType() == QueryType.PPL) {
            PPLQueryHelper.execute(transformedRequest, listener);
        } else if (transformedRequest.getType() == QueryType.SQL) {
            SQLQueryHelper.execute(transformedRequest, listener);
        }
    }
}
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.transport;

import org.json.JSONObject;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.commons.sql.action.TransportQueryRequest;
import org.opensearch.commons.sql.action.TransportQueryResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.domain.SQLQueryRequest;

import java.util.Collections;

import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

public class SQLQueryHelper {
    private static SQLService sqlService;
    private static SQLQueryHelper INSTANCE;

    public static synchronized SQLQueryHelper getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SQLQueryHelper();
        }
        return INSTANCE;
    }

    public void setSqlService(SQLService sqlService) {
        SQLQueryHelper.sqlService = sqlService;
    }

    public static void execute(TransportQueryRequest request, ActionListener<TransportQueryResponse> listener) {
        // convert the TransportQueryRequest request to SQLQueryRequest
        SQLQueryRequest sqlRequest = createSQLQueryRequest(request);
        // execute by sql service
        sqlService.execute(sqlRequest, createListener(sqlRequest, listener));
    }

    private static ResponseListener<ExecutionEngine.QueryResponse> createListener(
            SQLQueryRequest sqlRequest,
            ActionListener<TransportQueryResponse> listener
    ) {
        Format format = sqlRequest.format();
        ResponseFormatter<QueryResult> formatter;
        if (format.equals(Format.CSV)) {
            formatter = new CsvResponseFormatter(sqlRequest.sanitize());
        } else if (format.equals(Format.RAW)) {
            formatter = new RawResponseFormatter();
        } else {
            formatter = new SimpleJsonResponseFormatter(PRETTY);
        }

        return new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse response) {
                String responseContent = formatter.format(new QueryResult(response.getSchema(),
                        response.getResults()));
                sendResponse(responseContent, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (isClientError(e)) {
                    Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS).increment();
                    reportError(listener, e, BAD_REQUEST);
                } else {
                    Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS).increment();
                    reportError(listener, e, SERVICE_UNAVAILABLE);
                }
            }
        };
    }

    private static void sendResponse(String content, ActionListener<TransportQueryResponse> listener) {
        listener.onResponse(new TransportQueryResponse(content));
    }

    private static void reportError(ActionListener<TransportQueryResponse> listener, final Exception exception, final RestStatus status) {
        listener.onFailure(new OpenSearchStatusException(exception.getMessage(), status));
    }

    private static boolean isClientError(Exception e) {
        return e instanceof NullPointerException
                // NPE is hard to differentiate but more likely caused by bad query
                || e instanceof IllegalArgumentException
                || e instanceof IndexNotFoundException
                || e instanceof SemanticCheckException
                || e instanceof ExpressionEvaluationException
                || e instanceof QueryEngineException
                || e instanceof SyntaxCheckException;
    }

    private static SQLQueryRequest createSQLQueryRequest(TransportQueryRequest request) {
        String query = request.getQuery();
        String jsonContent = "{\"query\": \"" + query + "\"}";
        return new SQLQueryRequest(new JSONObject(jsonContent), query ,"/_plugins/_sql", Collections.emptyMap());
    }
}
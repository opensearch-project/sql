package org.opensearch.sql.plugin.transport;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.commons.authuser.User;
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
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;

import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

public class PPLQueryHelper {
    private static PPLService pplService;
    private static PPLQueryHelper INSTANCE;

    public static synchronized PPLQueryHelper getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PPLQueryHelper();
        }
        return INSTANCE;
    }

    public void setPplService(PPLService pplService) {
        PPLQueryHelper.pplService = pplService;
    }

    public static void execute(TransportQueryRequest request, ActionListener<TransportQueryResponse> listener) {
        User user = User.parse(request.getQuery());

        // convert the TransportQueryRequest request to PPLQueryRequest
        PPLQueryRequest pplRequest = createPPLQueryRequest(request);
        // execute by ppl service
        pplService.execute(pplRequest, createListener(pplRequest, listener));
    }

    private static ResponseListener<ExecutionEngine.QueryResponse> createListener(
            PPLQueryRequest pplRequest,
            ActionListener<TransportQueryResponse> listener
    ) {
        Format format = pplRequest.format();
        ResponseFormatter<QueryResult> formatter;
        if (format.equals(Format.CSV)) {
            formatter = new CsvResponseFormatter(pplRequest.sanitize());
        } else if (format.equals(Format.RAW)) {
            formatter = new RawResponseFormatter();
        } else if (format.equals(Format.VIZ)) {
            formatter = new VisualizationResponseFormatter(pplRequest.style());
        } else {
            formatter = new SimpleJsonResponseFormatter(PRETTY);
        }

        return new ResponseListener<ExecutionEngine.QueryResponse>() {
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

    private static PPLQueryRequest createPPLQueryRequest(TransportQueryRequest request) {
        // TODO: Currently Using fixed default format JDBC
        return new PPLQueryRequest(request.getQuery(), null,"/_plugins/_ppl");
    }

}

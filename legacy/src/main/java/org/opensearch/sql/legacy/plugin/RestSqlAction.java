/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.collect.ImmutableList;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.legacy.antlr.OpenSearchLegacySqlAnalyzer;
import org.opensearch.sql.legacy.antlr.SqlAnalysisConfig;
import org.opensearch.sql.legacy.antlr.SqlAnalysisException;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.cursor.CursorType;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.domain.QueryActionRequest;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.ActionRequestRestExecutorFactory;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.executor.RestExecutor;
import org.opensearch.sql.legacy.executor.cursor.CursorActionRequestRestExecutorFactory;
import org.opensearch.sql.legacy.executor.cursor.CursorAsyncRestExecutor;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.legacy.request.SqlRequestFactory;
import org.opensearch.sql.legacy.request.SqlRequestParam;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;
import org.opensearch.sql.legacy.utils.JsonPrettyFormatter;
import org.opensearch.sql.legacy.utils.QueryDataAnonymizer;
import org.opensearch.sql.sql.domain.SQLQueryRequest;

public class RestSqlAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestSqlAction.class);

    private final boolean allowExplicitIndex;

    private static final Predicate<String> CONTAINS_SUBQUERY = Pattern.compile("\\(\\s*select ").asPredicate();

    /**
     * API endpoint path
     */
    public static final String QUERY_API_ENDPOINT = "/_plugins/_sql";
    public static final String EXPLAIN_API_ENDPOINT = QUERY_API_ENDPOINT + "/_explain";
    public static final String CURSOR_CLOSE_ENDPOINT = QUERY_API_ENDPOINT + "/close";
    public static final String LEGACY_QUERY_API_ENDPOINT = "/_opendistro/_sql";
    public static final String LEGACY_EXPLAIN_API_ENDPOINT = LEGACY_QUERY_API_ENDPOINT + "/_explain";
    public static final String LEGACY_CURSOR_CLOSE_ENDPOINT = LEGACY_QUERY_API_ENDPOINT + "/close";

    /**
     * New SQL query request handler.
     */
    private final RestSQLQueryAction newSqlQueryHandler;

    public RestSqlAction(Settings settings, Injector injector) {
        super();
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
        this.newSqlQueryHandler = new RestSQLQueryAction(injector);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList.of(
            new ReplacedRoute(
                RestRequest.Method.POST, QUERY_API_ENDPOINT,
                RestRequest.Method.POST, LEGACY_QUERY_API_ENDPOINT),
            new ReplacedRoute(
                RestRequest.Method.POST, EXPLAIN_API_ENDPOINT,
                RestRequest.Method.POST, LEGACY_EXPLAIN_API_ENDPOINT),
            new ReplacedRoute(
                RestRequest.Method.POST, CURSOR_CLOSE_ENDPOINT,
                RestRequest.Method.POST, LEGACY_CURSOR_CLOSE_ENDPOINT));
    }

    @Override
    public String getName() {
        return "sql_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_TOTAL).increment();
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_COUNT_TOTAL).increment();

        QueryContext.addRequestId();

        try {
            if (!isSQLFeatureEnabled()) {
                throw new SQLFeatureDisabledException(
                        "Either plugins.sql.enabled or rest.action.multi.allow_explicit_index setting is false"
                );
            }

            final SqlRequest sqlRequest = SqlRequestFactory.getSqlRequest(request);
            if (isLegacyCursor(sqlRequest)) {
                if (isExplainRequest(request)) {
                    throw new IllegalArgumentException("Invalid request. Cannot explain cursor");
                } else {
                    LOG.info("[{}] Cursor request {}: {}", QueryContext.getRequestId(), request.uri(), sqlRequest.cursor());
                    return channel -> handleCursorRequest(request, sqlRequest.cursor(), client, channel);
                }
            }

            LOG.info("[{}] Incoming request {}", QueryContext.getRequestId(), request.uri());

            Format format = SqlRequestParam.getFormat(request.params());

            // Route request to new query engine if it's supported already
            SQLQueryRequest newSqlRequest = new SQLQueryRequest(sqlRequest.getJsonContent(),
                sqlRequest.getSql(), request.path(), request.params(), sqlRequest.cursor());
            return newSqlQueryHandler.prepareRequest(newSqlRequest,
                    (restChannel, exception) -> {
                        try{
                            if (newSqlRequest.isExplainRequest()) {
                                LOG.info("Request is falling back to old SQL engine due to: " + exception.getMessage());
                            }
                            LOG.info("[{}] Request {} is not supported and falling back to old SQL engine",
                                QueryContext.getRequestId(), newSqlRequest);
                            LOG.info("Request Query: {}", QueryDataAnonymizer.anonymizeData(sqlRequest.getSql()));
                            QueryAction queryAction = explainRequest(client, sqlRequest, format);
                            executeSqlRequest(request, queryAction, client, restChannel);
                        } catch (Exception e) {
                            logAndPublishMetrics(e);
                            reportError(restChannel, e, isClientError(e) ? BAD_REQUEST : SERVICE_UNAVAILABLE);
                        }
                    },
                    (restChannel, exception) -> {
                        logAndPublishMetrics(exception);
                        reportError(restChannel, exception, isClientError(exception) ?
                            BAD_REQUEST : SERVICE_UNAVAILABLE);
                    });
        } catch (Exception e) {
            logAndPublishMetrics(e);
            return channel -> reportError(channel, e, isClientError(e) ? BAD_REQUEST : SERVICE_UNAVAILABLE);
        }
    }


    /**
     * @param sqlRequest client request
     * @return true if this cursor was generated by the legacy engine, false otherwise.
     */
    private static boolean isLegacyCursor(SqlRequest sqlRequest) {
        String cursor = sqlRequest.cursor();
        return cursor != null
            && CursorType.getById(cursor.substring(0, 1)) != CursorType.NULL;
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format", "sanitize"));
        return responseParams;
    }

    private void handleCursorRequest(final RestRequest request, final String cursor, final Client client,
                                     final RestChannel channel) throws Exception {
        CursorAsyncRestExecutor cursorRestExecutor = CursorActionRequestRestExecutorFactory.createExecutor(
                request, cursor, SqlRequestParam.getFormat(request.params()));
        cursorRestExecutor.execute(client, request.params(), channel);
    }

    private static void logAndPublishMetrics(final Exception e) {
        if (isClientError(e)) {
            LOG.error(QueryContext.getRequestId() + " Client side error during query execution", e);
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
        } else {
            LOG.error(QueryContext.getRequestId() + " Server side error during query execution", e);
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        }
    }

    private static QueryAction explainRequest(final NodeClient client, final SqlRequest sqlRequest, Format format)
        throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {

        ColumnTypeProvider typeProvider = performAnalysis(sqlRequest.getSql());

        final QueryAction queryAction = new SearchDao(client)
                .explain(new QueryActionRequest(sqlRequest.getSql(), typeProvider, format));
        queryAction.setSqlRequest(sqlRequest);
        queryAction.setFormat(format);
        queryAction.setColumnTypeProvider(typeProvider);
        return queryAction;
    }

    private void executeSqlRequest(final RestRequest request, final QueryAction queryAction, final Client client,
                                   final RestChannel channel) throws Exception {
        Map<String, String> params = request.params();
        if (isExplainRequest(request)) {
            final String jsonExplanation = queryAction.explain().explain();
            String result;
            if (SqlRequestParam.isPrettyFormat(params)) {
                result = JsonPrettyFormatter.format(jsonExplanation);
            } else {
                result = jsonExplanation;
            }
            channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8", result));
        } else {
            RestExecutor restExecutor = ActionRequestRestExecutorFactory.createExecutor(
                    SqlRequestParam.getFormat(params),
                    queryAction);
            //doing this hack because OpenSearch throws exception for un-consumed props
            Map<String, String> additionalParams = new HashMap<>();
            for (String paramName : responseParams()) {
                if (request.hasParam(paramName)) {
                    additionalParams.put(paramName, request.param(paramName));
                }
            }
            restExecutor.execute(client, additionalParams, queryAction, channel);
        }
    }

    private static boolean isExplainRequest(final RestRequest request) {
        return request.path().endsWith("/_explain");
    }

    private static boolean isClientError(Exception e) {
        return e instanceof NullPointerException // NPE is hard to differentiate but more likely caused by bad query
            || e instanceof SqlParseException
            || e instanceof ParserException
            || e instanceof SQLFeatureNotSupportedException
            || e instanceof SQLFeatureDisabledException
            || e instanceof IllegalArgumentException
            || e instanceof IndexNotFoundException
            || e instanceof VerificationException
            || e instanceof SqlAnalysisException
            || e instanceof SyntaxCheckException
            || e instanceof SemanticCheckException
            || e instanceof ExpressionEvaluationException;
    }

    private void sendResponse(final RestChannel channel, final String message, final RestStatus status) {
        channel.sendResponse(new BytesRestResponse(status, message));
    }

    private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
        sendResponse(channel, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString(), status);
    }

    private boolean isSQLFeatureEnabled() {
        boolean isSqlEnabled = LocalClusterState.state().getSettingValue(
            org.opensearch.sql.common.setting.Settings.Key.SQL_ENABLED);
        return allowExplicitIndex && isSqlEnabled;
    }

    private static ColumnTypeProvider performAnalysis(String sql) {
        LocalClusterState clusterState = LocalClusterState.state();
        SqlAnalysisConfig config = new SqlAnalysisConfig(false, false, 200);

        OpenSearchLegacySqlAnalyzer analyzer = new OpenSearchLegacySqlAnalyzer(config);
        Optional<Type> outputColumnType = analyzer.analyze(sql, clusterState);
        if (outputColumnType.isPresent()) {
            return new ColumnTypeProvider(outputColumnType.get());
        } else {
            return new ColumnTypeProvider();
        }
    }
}

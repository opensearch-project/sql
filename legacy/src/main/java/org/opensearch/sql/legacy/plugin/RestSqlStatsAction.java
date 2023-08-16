/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.metrics.Metrics;

/**
 * Currently this interface is for node level.
 * Cluster level is coming up soon. https://github.com/opendistro-for-elasticsearch/sql/issues/41
 */
public class RestSqlStatsAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(RestSqlStatsAction.class);

    /**
     * API endpoint path
     */
    public static final String STATS_API_ENDPOINT = "/_plugins/_sql/stats";
    public static final String LEGACY_STATS_API_ENDPOINT = "/_opendistro/_sql/stats";

    public RestSqlStatsAction(Settings settings, RestController restController) {
        super();
    }

    @Override
    public String getName() {
        return "sql_stats_action";
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList.of(
            new ReplacedRoute(
                RestRequest.Method.POST, STATS_API_ENDPOINT,
                RestRequest.Method.POST, LEGACY_STATS_API_ENDPOINT),
            new ReplacedRoute(
                RestRequest.Method.GET, STATS_API_ENDPOINT,
                RestRequest.Method.GET, LEGACY_STATS_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

        QueryContext.addRequestId();

        try {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK,
                    Metrics.getInstance().collectToJSON()));
        } catch (Exception e) {
            LOG.error("Failed during Query SQL STATS Action.", e);

            return channel -> channel.sendResponse(new BytesRestResponse(SERVICE_UNAVAILABLE,
                    ErrorMessageFactory.createErrorMessage(e, SERVICE_UNAVAILABLE.getStatus()).toString()));
        }
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format", "sanitize"));
        return responseParams;
    }

}

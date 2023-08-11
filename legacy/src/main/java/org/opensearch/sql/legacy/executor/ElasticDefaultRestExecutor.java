/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.executor;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.reindex.BulkIndexByScrollResponseContentListener;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.join.ElasticJoinExecutor;
import org.opensearch.sql.legacy.executor.join.ElasticUtils;
import org.opensearch.sql.legacy.executor.join.MetaSearchResult;
import org.opensearch.sql.legacy.executor.multi.MultiRequestExecutorFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.join.JoinRequestBuilder;
import org.opensearch.sql.legacy.query.multi.MultiQueryRequestBuilder;


public class ElasticDefaultRestExecutor implements RestExecutor {

    /**
     * Request builder to generate OpenSearch DSL
     */
    private final SqlElasticRequestBuilder requestBuilder;

    private static final Logger LOG = LogManager.getLogger(ElasticDefaultRestExecutor.class);

    public ElasticDefaultRestExecutor(QueryAction queryAction) {
        // Put explain() here to make it run in NIO thread
        try {
            this.requestBuilder = queryAction.explain();
        } catch (SqlParseException e) {
            throw new IllegalStateException("Failed to explain query action", e);
        }
    }

    /**
     * Execute the ActionRequest and returns the REST response using the channel.
     */
    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel)
            throws Exception {
        ActionRequest request = requestBuilder.request();

        if (requestBuilder instanceof JoinRequestBuilder) {
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            executor.sendResponse(channel);
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client,
                    (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            sendDefaultResponse(executor.getHits(), channel);
        } else if (request instanceof SearchRequest) {
            client.search((SearchRequest) request, new RestStatusToXContentListener<>(channel));
        } else if (request instanceof DeleteByQueryRequest) {
            requestBuilder.getBuilder().execute(
                    new BulkIndexByScrollResponseContentListener(channel, Maps.newHashMap()));
        } else if (request instanceof GetIndexRequest) {
            requestBuilder.getBuilder().execute(new GetIndexRequestRestListener(channel, (GetIndexRequest) request));
        } else if (request instanceof SearchScrollRequest) {
            client.searchScroll((SearchScrollRequest) request, new RestStatusToXContentListener<>(channel));
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }
    }

    @Override
    public String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception {
        ActionRequest request = requestBuilder.request();

        if (requestBuilder instanceof JoinRequestBuilder) {
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client,
                    (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (request instanceof SearchRequest) {
            ActionFuture<SearchResponse> future = client.search((SearchRequest) request);
            SearchResponse response = future.actionGet();
            return response.toString();
        } else if (request instanceof DeleteByQueryRequest) {
            return requestBuilder.get().toString();
        } else if (request instanceof GetIndexRequest) {
            return requestBuilder.getBuilder().execute().actionGet().toString();
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }

    }

    private void sendDefaultResponse(SearchHits hits, RestChannel channel) {
        try {
            String json = ElasticUtils.hitsAsStringResult(hits, new MetaSearchResult());
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, json);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

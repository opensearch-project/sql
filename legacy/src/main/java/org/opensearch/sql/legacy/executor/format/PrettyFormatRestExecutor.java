/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.executor.format;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.legacy.cursor.Cursor;
import org.opensearch.sql.legacy.cursor.DefaultCursor;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.QueryActionElasticExecutor;
import org.opensearch.sql.legacy.executor.RestExecutor;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;

public class PrettyFormatRestExecutor implements RestExecutor {

    private static final Logger LOG = LogManager.getLogger();

    private final String format;

    public PrettyFormatRestExecutor(String format) {
        this.format = format.toLowerCase();
    }

    /**
     * Execute the QueryAction and return the REST response using the channel.
     */
    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) {
        String formattedResponse = execute(client, params, queryAction);
        BytesRestResponse bytesRestResponse;
        if (format.equals("jdbc")) {
            bytesRestResponse = new BytesRestResponse(RestStatus.OK,
                    "application/json; charset=UTF-8",
                    formattedResponse);
        } else {
            bytesRestResponse = new BytesRestResponse(RestStatus.OK, formattedResponse);
        }

        if (!BackOffRetryStrategy.isHealthy(2 * bytesRestResponse.content().length(), this)) {
            throw new IllegalStateException(
                    "[PrettyFormatRestExecutor] Memory could be insufficient when sendResponse().");
        }

        channel.sendResponse(bytesRestResponse);
    }

    @Override
    public String execute(Client client, Map<String, String> params, QueryAction queryAction) {
        Protocol protocol;

        try {
            if (queryAction instanceof DefaultQueryAction) {
                protocol = buildProtocolForDefaultQuery(client, (DefaultQueryAction) queryAction);
            } else {
                Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
                protocol = new Protocol(client, queryAction, queryResult, format, Cursor.NULL_CURSOR);
            }
        } catch (Exception e) {
            if (e instanceof OpenSearchException) {
                LOG.warn("An error occurred in OpenSearch engine: "
                        + ((OpenSearchException) e).getDetailedMessage(), e);
            } else {
                LOG.warn("Error happened in pretty formatter", e);
            }
            protocol = new Protocol(e);
        }

        return protocol.format();
    }

    /**
     * QueryActionElasticExecutor.executeAnyAction() returns SearchHits inside SearchResponse.
     * In order to get scroll ID if any, we need to execute DefaultQueryAction ourselves for SearchResponse.
     */
    private Protocol buildProtocolForDefaultQuery(Client client, DefaultQueryAction queryAction)
            throws SqlParseException {

        SearchResponse response = (SearchResponse) queryAction.explain().get();
        String scrollId = response.getScrollId();

        Protocol protocol;
        if (!Strings.isNullOrEmpty(scrollId)) {
            DefaultCursor defaultCursor = new DefaultCursor();
            defaultCursor.setScrollId(scrollId);
            defaultCursor.setLimit(queryAction.getSelect().getRowCount());
            defaultCursor.setFetchSize(queryAction.getSqlRequest().fetchSize());
            protocol = new Protocol(client, queryAction, response.getHits(), format, defaultCursor);
        } else {
            protocol = new Protocol(client, queryAction, response.getHits(), format, Cursor.NULL_CURSOR);
        }

        return protocol;
    }
}

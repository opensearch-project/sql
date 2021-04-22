/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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

package com.amazon.opendistroforelasticsearch.sql.legacy.plugin;

import com.amazon.opendistroforelasticsearch.sql.legacy.executor.format.ErrorMessageFactory;
import com.amazon.opendistroforelasticsearch.sql.legacy.utils.LogUtils;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rest.RestStatus.INTERNAL_SERVER_ERROR;

/**
 * Interface to manage opensearch.sql.* cluster settings
 * All non-sql settings are ignored.
 * Any non-transient and non-persistent settings are ignored.
 */
public class RestSqlSettingsAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(RestSqlSettingsAction.class);

    private static final String PERSISTENT = "persistent";
    private static final String TRANSIENT = "transient";
    private static final String SQL_SETTINGS_PREFIX = "opensearch.sql.";

    /**
     * API endpoint path
     */
    public static final String SETTINGS_API_ENDPOINT = "/_opensearch/_sql/settings";
    public static final String LEGACY_SETTINGS_API_ENDPOINT = "/_opendistro/_sql/settings";

    public RestSqlSettingsAction(Settings settings, RestController restController) {
        super();
    }

    @Override
    public String getName() {
        return "sql_settings_action";
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
                new Route(RestRequest.Method.PUT, SETTINGS_API_ENDPOINT),
                new Route(RestRequest.Method.PUT, LEGACY_SETTINGS_API_ENDPOINT)
        );
    }

    /**
     * @see org.opensearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        LogUtils.addRequestId();
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.timeout(request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
        clusterUpdateSettingsRequest.masterNodeTimeout(
                request.paramAsTime("master_timeout", clusterUpdateSettingsRequest.masterNodeTimeout()));
        Map<String, Object> source;
        try (XContentParser parser = request.contentParser()) {
            source = parser.map();
        }

        try {
            if (source.containsKey(TRANSIENT)) {
                clusterUpdateSettingsRequest.transientSettings(getAndFilterSettings((Map) source.get(TRANSIENT)));
            }
            if (source.containsKey(PERSISTENT)) {
                clusterUpdateSettingsRequest.persistentSettings(getAndFilterSettings((Map) source.get(PERSISTENT)));
            }

            return channel -> client.admin().cluster().updateSettings(
                    clusterUpdateSettingsRequest, new RestToXContentListener<>(channel));
        } catch (Exception e) {
            LOG.error("Error changing OpenDistro SQL plugin cluster settings", e);
            return channel -> channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR,
                    ErrorMessageFactory.createErrorMessage(e, INTERNAL_SERVER_ERROR.getStatus()).toString()));
        }
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format"));
        return responseParams;
    }

    private Settings getAndFilterSettings(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            Settings.Builder settingsBuilder = Settings.builder().
                    loadFromSource(Strings.toString(builder), builder.contentType());
            settingsBuilder.keys().removeIf(key -> !key.startsWith(SQL_SETTINGS_PREFIX));
            return settingsBuilder.build();
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.transport.client.Requests;
import org.opensearch.transport.client.node.NodeClient;

public class RestQuerySettingsAction extends BaseRestHandler {
  private static final Logger LOG = LogManager.getLogger(RestQuerySettingsAction.class);
  private static final String PERSISTENT = "persistent";
  private static final String TRANSIENT = "transient";
  private static final String SQL_SETTINGS_PREFIX = "plugins.sql.";
  private static final String PPL_SETTINGS_PREFIX = "plugins.ppl.";
  private static final String CALCITE_SETTINGS_PREFIX = "plugins.calcite.";
  private static final String COMMON_SETTINGS_PREFIX = "plugins.query.";
  private static final String EXECUTION_ENGINE_SETTINGS_PREFIX = "plugins.query.executionengine";
  public static final String DATASOURCES_SETTINGS_PREFIX = "plugins.query.datasources";
  private static final List<String> SETTINGS_PREFIX =
      ImmutableList.of(
          SQL_SETTINGS_PREFIX,
          PPL_SETTINGS_PREFIX,
          COMMON_SETTINGS_PREFIX,
          CALCITE_SETTINGS_PREFIX);

  private static final List<String> DENY_LIST_SETTINGS_PREFIX =
      ImmutableList.of(EXECUTION_ENGINE_SETTINGS_PREFIX, DATASOURCES_SETTINGS_PREFIX);

  public static final String SETTINGS_API_ENDPOINT = "/_plugins/_query/settings";

  public RestQuerySettingsAction(Settings settings, RestController restController) {
    super();
  }

  @Override
  public String getName() {
    return "ppl_settings_action";
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(RestRequest.Method.PUT, SETTINGS_API_ENDPOINT));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
      throws IOException {
    QueryContext.addRequestId();
    final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest =
        Requests.clusterUpdateSettingsRequest();
    clusterUpdateSettingsRequest.timeout(
        request.paramAsTime("timeout", clusterUpdateSettingsRequest.timeout()));
    clusterUpdateSettingsRequest.clusterManagerNodeTimeout(
        request.paramAsTime(
            "cluster_manager_timeout", clusterUpdateSettingsRequest.clusterManagerNodeTimeout()));
    Map<String, Object> source;
    try (XContentParser parser = request.contentParser()) {
      source = parser.map();
    }

    try {
      if (source.containsKey(TRANSIENT)) {
        clusterUpdateSettingsRequest.transientSettings(
            getAndFilterSettings((Map<String, ?>) source.get(TRANSIENT)));
      }
      if (source.containsKey(PERSISTENT)) {
        clusterUpdateSettingsRequest.persistentSettings(
            getAndFilterSettings((Map<String, ?>) source.get(PERSISTENT)));
      }

      return channel ->
          client
              .admin()
              .cluster()
              .updateSettings(clusterUpdateSettingsRequest, new RestToXContentListener<>(channel));
    } catch (Exception e) {
      LOG.error("Error changing OpenSearch SQL plugin cluster settings", e);
      return channel ->
          channel.sendResponse(
              new BytesRestResponse(
                  INTERNAL_SERVER_ERROR,
                  ErrorMessageFactory.createErrorMessage(e, INTERNAL_SERVER_ERROR.getStatus())
                      .toString()));
    }
  }

  private Settings getAndFilterSettings(Map<String, ?> source) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.map(source);
      Settings.Builder settingsBuilder =
          Settings.builder().loadFromSource(builder.toString(), builder.contentType());
      settingsBuilder
          .keys()
          .removeIf(
              key -> {
                for (String prefix : SETTINGS_PREFIX) {
                  if (key.startsWith(prefix)) {
                    return false;
                  }
                }
                return true;
              });
      // Applying DenyList Filter.
      settingsBuilder
          .keys()
          .removeIf(key -> DENY_LIST_SETTINGS_PREFIX.stream().anyMatch(key::startsWith));
      return settingsBuilder.build();
    } catch (IOException e) {
      throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
    }
  }
}

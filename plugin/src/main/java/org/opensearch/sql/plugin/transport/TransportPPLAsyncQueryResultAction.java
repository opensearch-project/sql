/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.json.JSONObject;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Transport implementation of GET {@code /_plugins/_ppl/jobs/{id}}. */
public class TransportPPLAsyncQueryResultAction extends TransportPPLAsyncQueryLifecycleAction {
  private final org.opensearch.sql.common.setting.Settings pluginSettings;

  @Inject
  public TransportPPLAsyncQueryResultAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      PPLAsyncQueryJobService jobService) {
    super(
        PPLAsyncQueryResultAction.NAME,
        transportService,
        actionFilters,
        client,
        clusterService,
        jobService);
    this.pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
  }

  @Override
  protected void handleLocal(
      TransportPPLQueryRequest request,
      User user,
      ActionListener<TransportPPLQueryResponse> listener) {
    JSONObject json = json(request);
    String id = requestedId(request);
    TimeValue keepAlive =
        json.has("keep_alive")
            ? TimeValue.parseTimeValue(
                json.getString("keep_alive"),
                PPLAsyncQueryJobService.DEFAULT_KEEP_ALIVE,
                "keep_alive")
            : null;
    if (keepAlive != null) {
      PPLAsyncQueryJobService.validateKeepAlive(keepAlive);
    }
    int offset = intParameter(json, "offset", 0);
    if (offset < 0) {
      throw new IllegalArgumentException("[offset] must be greater than or equal to 0");
    }
    int count = intParameter(json, "count", PPLAsyncQueryJobService.DEFAULT_PAGE_SIZE);
    int maxPageSize = pluginSettings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_PAGE_SIZE);
    if (count < 1 || count > maxPageSize) {
      throw new IllegalArgumentException("[count] must be between 1 and " + maxPageSize);
    }

    PPLAsyncQueryJobService.Snapshot snapshot = jobService.get(id, user, keepAlive);
    if (snapshot.status() == PPLAsyncQueryJobService.Status.RUNNING
        && snapshot.classified()
        && snapshot.updateMode() == UpdateMode.REPLACE
        && offset != 0) {
      throw new IllegalArgumentException("[offset] must be 0 for a running REPLACE result");
    }
    listener.onResponse(PPLAsyncQueryResponseFormatter.job(snapshot, offset, count));
  }

  private static int intParameter(JSONObject json, String name, int defaultValue) {
    if (!json.has(name)) {
      return defaultValue;
    }
    long value;
    try {
      value = Long.parseLong(json.get(name).toString());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("[" + name + "] must be an integer", e);
    }
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("[" + name + "] is outside the integer range");
    }
    return (int) value;
  }
}

/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import java.util.Locale;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCreateAsyncQueryRequestAction
    extends HandledTransportAction<CreateAsyncQueryActionRequest, CreateAsyncQueryActionResponse> {

  private final AsyncQueryExecutorService asyncQueryExecutorService;
  private final OpenSearchSettings pluginSettings;

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/create";
  public static final ActionType<CreateAsyncQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CreateAsyncQueryActionResponse::new);

  @Inject
  public TransportCreateAsyncQueryRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      AsyncQueryExecutorServiceImpl jobManagementService,
      OpenSearchSettings pluginSettings) {
    super(NAME, transportService, actionFilters, CreateAsyncQueryActionRequest::new);
    this.asyncQueryExecutorService = jobManagementService;
    this.pluginSettings = pluginSettings;
  }

  @Override
  protected void doExecute(
      Task task,
      CreateAsyncQueryActionRequest request,
      ActionListener<CreateAsyncQueryActionResponse> listener) {
    try {
      if (!(Boolean) pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)) {
        listener.onFailure(
            new IllegalAccessException(
                String.format(
                    Locale.ROOT,
                    "%s setting is " + "false",
                    Settings.Key.ASYNC_QUERY_ENABLED.getKeyValue())));
        return;
      }

      CreateAsyncQueryRequest createAsyncQueryRequest = request.getCreateAsyncQueryRequest();
      CreateAsyncQueryResponse createAsyncQueryResponse =
          asyncQueryExecutorService.createAsyncQuery(createAsyncQueryRequest);
      String responseContent =
          new JsonResponseFormatter<CreateAsyncQueryResponse>(JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(CreateAsyncQueryResponse response) {
              return response;
            }
          }.format(createAsyncQueryResponse);
      listener.onResponse(new CreateAsyncQueryActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}

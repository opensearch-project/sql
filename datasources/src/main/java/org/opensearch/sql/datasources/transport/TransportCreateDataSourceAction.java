/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

public class TransportCreateDataSourceAction
    extends HandledTransportAction<CreateDataSourceActionRequest, CreateDataSourceActionResponse> {
  public static final String NAME = "cluster:admin/opensearch/ql/datasources/create";
  public static final ActionType<CreateDataSourceActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CreateDataSourceActionResponse::new);

  private DataSourceService dataSourceService;

  /**
   * TransportCreateDataSourceAction action for creating datasource.
   *
   * @param transportService transportService.
   * @param actionFilters actionFilters.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportCreateDataSourceAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DataSourceServiceImpl dataSourceService) {
    super(
        TransportCreateDataSourceAction.NAME,
        transportService,
        actionFilters,
        CreateDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(
      Task task,
      CreateDataSourceActionRequest request,
      ActionListener<CreateDataSourceActionResponse> actionListener) {
    try {
      DataSourceMetadata dataSourceMetadata = request.getDataSourceMetadata();
      dataSourceService.createDataSource(dataSourceMetadata);
      String responseContent =
              new JsonResponseFormatter<String>(PRETTY) {
                @Override
                protected Object buildJsonObject(String response) {
                  return response;
                }
              }.format("Created DataSource with name " + dataSourceMetadata.getName());
      actionListener.onResponse(
          new CreateDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }
}

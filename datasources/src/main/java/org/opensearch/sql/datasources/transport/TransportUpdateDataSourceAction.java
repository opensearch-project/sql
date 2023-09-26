/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.transport;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportUpdateDataSourceAction
    extends HandledTransportAction<UpdateDataSourceActionRequest, UpdateDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/datasources/update";
  public static final ActionType<UpdateDataSourceActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, UpdateDataSourceActionResponse::new);

  private DataSourceService dataSourceService;

  /**
   * TransportUpdateDataSourceAction action for updating datasource.
   *
   * @param transportService transportService.
   * @param actionFilters actionFilters.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportUpdateDataSourceAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DataSourceServiceImpl dataSourceService) {
    super(
        TransportUpdateDataSourceAction.NAME,
        transportService,
        actionFilters,
        UpdateDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(
      Task task,
      UpdateDataSourceActionRequest request,
      ActionListener<UpdateDataSourceActionResponse> actionListener) {
    try {
      dataSourceService.updateDataSource(request.getDataSourceMetadata());
      String responseContent =
          new JsonResponseFormatter<CreateUpdateDatasourceResponse>(PRETTY) {
            @Override
            protected Object buildJsonObject(CreateUpdateDatasourceResponse response) {
              return response;
            }
          }.format(
              new CreateUpdateDatasourceResponse(
                  "Updated DataSource with name " + request.getDataSourceMetadata().getName()));
      actionListener.onResponse(new UpdateDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }
}

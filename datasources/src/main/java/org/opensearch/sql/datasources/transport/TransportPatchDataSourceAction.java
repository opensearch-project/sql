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
import org.opensearch.sql.datasources.model.transport.PatchDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.PatchDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportPatchDataSourceAction
    extends HandledTransportAction<PatchDataSourceActionRequest, PatchDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/datasources/update";
  public static final ActionType<PatchDataSourceActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, PatchDataSourceActionResponse::new);

  private DataSourceService dataSourceService;

  /**
   * TransportUpdateDataSourceAction action for updating datasource.
   *
   * @param transportService transportService.
   * @param actionFilters actionFilters.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportPatchDataSourceAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DataSourceServiceImpl dataSourceService) {
    super(
        TransportPatchDataSourceAction.NAME,
        transportService,
        actionFilters,
        PatchDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(
      Task task,
      PatchDataSourceActionRequest request,
      ActionListener<PatchDataSourceActionResponse> actionListener) {
    try {
      dataSourceService.updateDataSource(request.getDataSourceMetadata());
      String responseContent =
          new JsonResponseFormatter<String>(PRETTY) {
            @Override
            protected Object buildJsonObject(String response) {
              return response;
            }
          }.format("Updated DataSource with name " + request.getDataSourceMetadata().getName());
      actionListener.onResponse(new PatchDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }
}

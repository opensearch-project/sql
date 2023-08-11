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
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportDeleteDataSourceAction
    extends HandledTransportAction<DeleteDataSourceActionRequest, DeleteDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/datasources/delete";
  public static final ActionType<DeleteDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, DeleteDataSourceActionResponse::new);

  private DataSourceService dataSourceService;

  /**
   * TransportDeleteDataSourceAction action for deleting datasource.
   *
   * @param transportService  transportService.
   * @param actionFilters     actionFilters.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportDeleteDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         DataSourceServiceImpl dataSourceService) {
    super(TransportDeleteDataSourceAction.NAME, transportService, actionFilters,
        DeleteDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(Task task, DeleteDataSourceActionRequest request,
                           ActionListener<DeleteDataSourceActionResponse> actionListener) {
    try {
      dataSourceService.deleteDataSource(request.getDataSourceName());
      actionListener.onResponse(new DeleteDataSourceActionResponse("Deleted DataSource with name "
          + request.getDataSourceName()));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }

}

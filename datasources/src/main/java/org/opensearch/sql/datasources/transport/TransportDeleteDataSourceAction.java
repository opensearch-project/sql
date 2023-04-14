/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceHolder;
import org.opensearch.sql.datasource.model.DataSourceInterfaceType;
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
   * @param dataSourceServiceHolder dataSourceServiceHolder.
   */
  @Inject
  public TransportDeleteDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         DataSourceServiceHolder dataSourceServiceHolder) {
    super(TransportDeleteDataSourceAction.NAME, transportService, actionFilters,
        DeleteDataSourceActionRequest::new);
    this.dataSourceService = dataSourceServiceHolder.getDataSourceService();
  }

  @Override
  protected void doExecute(Task task, DeleteDataSourceActionRequest request,
                           ActionListener<DeleteDataSourceActionResponse> actionListener) {
    if (dataSourceService.datasourceInterfaceType().equals(DataSourceInterfaceType.KEYSTORE)) {
      actionListener.onFailure(new UnsupportedOperationException(
          "Please set datasource interface settings(plugins.query.federation.datasources.interface)"
              + "to api in opensearch.yml to enable apis for datasource management. "
              + "Please port any datasources configured in keystore using create api."));
      return;
    }
    try {
      dataSourceService.deleteDataSource(request.getDataSourceName());
      actionListener.onResponse(new DeleteDataSourceActionResponse("Deleted DataSource with name "
          + request.getDataSourceName()));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }

}
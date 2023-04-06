/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.model.UpdateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.UpdateDataSourceActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportUpdateDataSourceAction
    extends HandledTransportAction<UpdateDataSourceActionRequest, UpdateDataSourceActionResponse> {

  private static final Logger LOG = LogManager.getLogger();
  public static final String NAME = "cluster:admin/opensearch/ql/datasources/update";
  public static final ActionType<UpdateDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, UpdateDataSourceActionResponse::new);

  private DataSourceService dataSourceService;
  private Client client;

  /**
   * TransportUpdateDataSourceAction action for updating datasource.
   *
   * @param transportService transportService.
   * @param actionFilters actionFilters.
   * @param client client.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportUpdateDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         NodeClient client,
                                         DataSourceServiceImpl dataSourceService) {
    super(TransportUpdateDataSourceAction.NAME, transportService, actionFilters,
        UpdateDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
    this.client = client;
  }

  @Override
  protected void doExecute(Task task, UpdateDataSourceActionRequest request,
                           ActionListener<UpdateDataSourceActionResponse> actionListener) {

    Metrics.getInstance().getNumericalMetric(MetricName.DATASOURCE_REQ_COUNT).increment();
    dataSourceService.updateDataSource(request.getDataSourceMetadata());
    actionListener.onResponse(new UpdateDataSourceActionResponse("Updated DataSource with name "
        + request.getDataSourceMetadata().getName()));
  }

}
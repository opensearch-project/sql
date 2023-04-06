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
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.plugin.model.CreateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.CreateDataSourceActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCreateDataSourceAction
    extends HandledTransportAction<CreateDataSourceActionRequest, CreateDataSourceActionResponse> {

  private static final Logger LOG = LogManager.getLogger();
  public static final String NAME = "cluster:admin/opensearch/ql/datasources/create";
  public static final ActionType<CreateDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, CreateDataSourceActionResponse::new);

  private DataSourceService dataSourceService;
  private Client client;

  /**
   * TransportCreateDataSourceAction action for creating datasource.
   *
   * @param transportService  transportService.
   * @param actionFilters     actionFilters.
   * @param client            client.
   * @param dataSourceService dataSourceService.
   */
  @Inject
  public TransportCreateDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         NodeClient client,
                                         DataSourceServiceImpl dataSourceService) {
    super(TransportCreateDataSourceAction.NAME, transportService, actionFilters,
        CreateDataSourceActionRequest::new);
    this.dataSourceService = dataSourceService;
    this.client = client;
  }

  @Override
  protected void doExecute(Task task, CreateDataSourceActionRequest request,
                           ActionListener<CreateDataSourceActionResponse> actionListener) {

    Metrics.getInstance().getNumericalMetric(MetricName.DATASOURCE_REQ_COUNT).increment();
    DataSourceMetadata dataSourceMetadata = request.getDataSourceMetadata();
    dataSourceService.createDataSource(dataSourceMetadata);
    actionListener.onResponse(new CreateDataSourceActionResponse("Created DataSource with name "
        + dataSourceMetadata.getName()));
  }

}
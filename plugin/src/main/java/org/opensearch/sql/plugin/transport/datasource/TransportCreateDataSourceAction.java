/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceMetadataStorage;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionRequest;
import org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class TransportCreateDataSourceAction extends HandledTransportAction<CreateDataSourceActionRequest, CreateDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/datasources/create";
  public static final ActionType<CreateDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, CreateDataSourceActionResponse::new);

  private AnnotationConfigApplicationContext applicationContext;

  @Inject
  public TransportCreateDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         AnnotationConfigApplicationContext applicationContext) {
    super(TransportCreateDataSourceAction.NAME, transportService, actionFilters, CreateDataSourceActionRequest::new);
    this.applicationContext = applicationContext;
  }

  @Override
  protected void doExecute(Task task, CreateDataSourceActionRequest request,
                           ActionListener<CreateDataSourceActionResponse> actionListener) {
    DataSourceService dataSourceService = this.applicationContext.getBean(DataSourceService.class);
    DataSourceMetadataStorage dataSourceMetadataStorage = this.applicationContext.getBean(DataSourceMetadataStorage.class);
    SecurityAccess.doPrivileged(() -> {
      dataSourceMetadataStorage.createDataSourceMetadata(request.getDataSourceMetadata());
      dataSourceService.addDataSource(request.getDataSourceMetadata());
      return null;
    });
    actionListener.onResponse(new CreateDataSourceActionResponse("Created DataSource"));
  }

}

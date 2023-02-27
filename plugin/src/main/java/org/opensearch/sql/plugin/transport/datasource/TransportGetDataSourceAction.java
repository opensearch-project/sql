/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.List;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceMetadataStorage;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.transport.datasource.model.GetDataSourceActionRequest;
import org.opensearch.sql.plugin.transport.datasource.model.GetDataSourceActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class TransportGetDataSourceAction
    extends HandledTransportAction<GetDataSourceActionRequest, GetDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/datasources/read";
  public static final ActionType<GetDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, GetDataSourceActionResponse::new);



  private AnnotationConfigApplicationContext applicationContext;

  @Inject
  public TransportGetDataSourceAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         AnnotationConfigApplicationContext applicationContext) {
    super(TransportGetDataSourceAction.NAME, transportService, actionFilters,
        GetDataSourceActionRequest::new);
    this.applicationContext = applicationContext;
  }

  @Override
  protected void doExecute(Task task, GetDataSourceActionRequest request,
                           ActionListener<GetDataSourceActionResponse> actionListener) {

    try {
      DataSourceMetadataStorage dataSourceMetadataStorage
          = this.applicationContext.getBean(DataSourceMetadataStorage.class);
      List<DataSourceMetadata> dataSourceMetadataSet
          = SecurityAccess.doPrivileged(dataSourceMetadataStorage::getDataSourceMetadata);
      String responseContent =
          new JsonResponseFormatter<List<DataSourceMetadata>>(PRETTY) {
            @Override
            protected Object buildJsonObject(List<DataSourceMetadata> response) {
              return response;
            }
          }.format(dataSourceMetadataSet);
      actionListener.onResponse(new GetDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }

  }
}

/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.transport;

import java.util.Set;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceHolder;
import org.opensearch.sql.datasource.model.DataSourceInterfaceType;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.model.transport.GetDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.GetDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetDataSourceAction
    extends HandledTransportAction<GetDataSourceActionRequest, GetDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/datasources/read";
  public static final ActionType<GetDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, GetDataSourceActionResponse::new);

  private DataSourceService dataSourceService;

  /**
   * TransportGetDataSourceAction action for getting datasource.
   *
   * @param transportService  transportService.
   * @param actionFilters     actionFilters.
   * @param dataSourceServiceHolder dataSourceServiceHolder.
   */
  @Inject
  public TransportGetDataSourceAction(TransportService transportService,
                                      ActionFilters actionFilters,
                                      DataSourceServiceHolder dataSourceServiceHolder) {
    super(TransportGetDataSourceAction.NAME, transportService, actionFilters,
        GetDataSourceActionRequest::new);
    this.dataSourceService = dataSourceServiceHolder.getDataSourceService();
  }

  @Override
  protected void doExecute(Task task, GetDataSourceActionRequest request,
                           ActionListener<GetDataSourceActionResponse> actionListener) {
    if (dataSourceService.datasourceInterfaceType().equals(DataSourceInterfaceType.KEYSTORE)) {
      actionListener.onFailure(new UnsupportedOperationException(
          "Please set datasource interface settings(plugins.query.federation.datasources.interface)"
              + "to api in opensearch.yml to enable apis for datasource management. "
              + "Please port any datasources configured in keystore using create api."));
      return;
    }
    try {
      String responseContent;
      if (request.getDataSourceName() == null) {
        responseContent = handleGetAllDataSourcesRequest();

      } else {
        responseContent = handleSingleDataSourceRequest(request.getDataSourceName());
      }
      actionListener.onResponse(new GetDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }

  private String handleGetAllDataSourcesRequest() {
    String responseContent;
    Set<DataSourceMetadata> dataSourceMetadataSet =
        dataSourceService.getDataSourceMetadata(false);
    responseContent = new JsonResponseFormatter<Set<DataSourceMetadata>>(
        JsonResponseFormatter.Style.PRETTY) {
      @Override
      protected Object buildJsonObject(Set<DataSourceMetadata> response) {
        return response;
      }
    }.format(dataSourceMetadataSet);
    return responseContent;
  }

  private String handleSingleDataSourceRequest(String datasourceName) {
    String responseContent;
    DataSourceMetadata dataSourceMetadata
        = dataSourceService
        .getDataSourceMetadata(datasourceName);
    responseContent = new JsonResponseFormatter<DataSourceMetadata>(
        JsonResponseFormatter.Style.PRETTY) {
      @Override
      protected Object buildJsonObject(DataSourceMetadata response) {
        return response;
      }
    }.format(dataSourceMetadata);
    return responseContent;
  }
}
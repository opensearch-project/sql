/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String CLUSTER_NAME_TAG_KEY = "domain_ident";
  public static final String JOB_TYPE_TAG_KEY = "type";

  private final EMRServerlessClientFactory emrServerlessClientFactory;
  private final DataSourceService dataSourceService;
  private final SessionManager sessionManager;
  private final QueryHandlerFactory queryHandlerFactory;

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    EMRServerlessClient emrServerlessClient = emrServerlessClientFactory.getClient();
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            dispatchQueryRequest.getDatasource());

    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())
        && SQLQueryUtils.isFlintExtensionQuery(dispatchQueryRequest.getQuery())) {
      IndexQueryDetails indexQueryDetails = getIndexQueryDetails(dispatchQueryRequest);
      DispatchQueryContext context = getDefaultDispatchContextBuilder(dispatchQueryRequest, dataSourceMetadata)
                      .indexQueryDetails(indexQueryDetails)
                      .build();

      return getQueryHandlerForFlintExtensionQuery(indexQueryDetails, emrServerlessClient)
              .submit(dispatchQueryRequest, context);
    } else {
      DispatchQueryContext context = getDefaultDispatchContextBuilder(dispatchQueryRequest, dataSourceMetadata)
                      .build();
      return getDefaultAsyncQueryHandler(emrServerlessClient).submit(dispatchQueryRequest, context);
    }
  }

  private static DispatchQueryContext.DispatchQueryContextBuilder getDefaultDispatchContextBuilder(DispatchQueryRequest dispatchQueryRequest, DataSourceMetadata dataSourceMetadata) {
    return DispatchQueryContext.builder()
            .dataSourceMetadata(dataSourceMetadata)
            .tags(getDefaultTagsForJobSubmission(dispatchQueryRequest))
            .queryId(AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()));
  }

  private AsyncQueryHandler getQueryHandlerForFlintExtensionQuery(IndexQueryDetails indexQueryDetails, EMRServerlessClient emrServerlessClient) {
    if (isEligibleForIndexDMLHandling(indexQueryDetails)) {
      return queryHandlerFactory.getIndexDMLHandler(emrServerlessClient);
    } else if (isEligibleForStreamingQuery(indexQueryDetails)) {
      return queryHandlerFactory.getStreamingQueryHandler(emrServerlessClient);
    } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
      // manual refresh should be handled by batch handler
      return queryHandlerFactory.getRefreshQueryHandler(emrServerlessClient);
    } else {
      return getDefaultAsyncQueryHandler(emrServerlessClient);
    }
  }

  @NotNull
  private AsyncQueryHandler getDefaultAsyncQueryHandler(EMRServerlessClient emrServerlessClient) {
    return sessionManager.isEnabled()
            ? queryHandlerFactory.getInteractiveQueryHandler()
            : queryHandlerFactory.getBatchQueryHandler(emrServerlessClient);
  }

  @NotNull
  private static IndexQueryDetails getIndexQueryDetails(DispatchQueryRequest dispatchQueryRequest) {
    IndexQueryDetails indexQueryDetails = SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
    fillDatasourceName(dispatchQueryRequest, indexQueryDetails);
    return indexQueryDetails;
  }


  private boolean isEligibleForStreamingQuery(IndexQueryDetails indexQueryDetails) {
    Boolean isCreateAutoRefreshIndex =
        IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())
            && indexQueryDetails.getFlintIndexOptions().autoRefresh();
    Boolean isAlterQuery =
        IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType());
    return isCreateAutoRefreshIndex || isAlterQuery;
  }

  private boolean isEligibleForIndexDMLHandling(IndexQueryDetails indexQueryDetails) {
    return IndexQueryActionType.DROP.equals(indexQueryDetails.getIndexQueryActionType())
        || IndexQueryActionType.VACUUM.equals(indexQueryDetails.getIndexQueryActionType())
        || (IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType())
            && (indexQueryDetails
                    .getFlintIndexOptions()
                    .getProvidedOptions()
                    .containsKey("auto_refresh")
                && !indexQueryDetails.getFlintIndexOptions().autoRefresh()));
  }

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata).getQueryResponse(asyncQueryJobMetadata);
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata).cancelJob(asyncQueryJobMetadata);
  }

  private AsyncQueryHandler getAsyncQueryHandlerForExistingQuery(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    EMRServerlessClient emrServerlessClient = emrServerlessClientFactory.getClient();
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return queryHandlerFactory.getInteractiveQueryHandler();
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      return queryHandlerFactory.getIndexDMLHandler(emrServerlessClient);
    } else if (asyncQueryJobMetadata.getJobType() == JobType.BATCH) {
      return queryHandlerFactory.getRefreshQueryHandler(emrServerlessClient);
    } else if (asyncQueryJobMetadata.getJobType() == JobType.STREAMING) {
      return queryHandlerFactory.getStreamingQueryHandler(emrServerlessClient);
    } else {
      return queryHandlerFactory.getBatchQueryHandler(emrServerlessClient);
    }
  }

  // TODO: Revisit this logic.
  // Currently, Spark if datasource is not provided in query.
  // Spark Assumes the datasource to be catalog.
  // This is required to handle drop index case properly when datasource name is not provided.
  private static void fillDatasourceName(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    if (indexQueryDetails.getFullyQualifiedTableName() != null
        && indexQueryDetails.getFullyQualifiedTableName().getDatasourceName() == null) {
      indexQueryDetails
          .getFullyQualifiedTableName()
          .setDatasourceName(dispatchQueryRequest.getDatasource());
    }
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }
}

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
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
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

  private final DataSourceService dataSourceService;
  private final SessionManager sessionManager;
  private final QueryHandlerFactory queryHandlerFactory;
  private final QueryIdProvider queryIdProvider;

  public DispatchQueryResponse dispatch(
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            dispatchQueryRequest.getDatasource());

    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())
        && SQLQueryUtils.isFlintExtensionQuery(dispatchQueryRequest.getQuery())) {
      IndexQueryDetails indexQueryDetails = getIndexQueryDetails(dispatchQueryRequest);
      DispatchQueryContext context =
          getDefaultDispatchContextBuilder(dispatchQueryRequest, dataSourceMetadata)
              .indexQueryDetails(indexQueryDetails)
              .asyncQueryRequestContext(asyncQueryRequestContext)
              .build();

      return getQueryHandlerForFlintExtensionQuery(indexQueryDetails)
          .submit(dispatchQueryRequest, context);
    } else {
      DispatchQueryContext context =
          getDefaultDispatchContextBuilder(dispatchQueryRequest, dataSourceMetadata)
              .asyncQueryRequestContext(asyncQueryRequestContext)
              .build();
      return getDefaultAsyncQueryHandler().submit(dispatchQueryRequest, context);
    }
  }

  private DispatchQueryContext.DispatchQueryContextBuilder getDefaultDispatchContextBuilder(
      DispatchQueryRequest dispatchQueryRequest, DataSourceMetadata dataSourceMetadata) {
    return DispatchQueryContext.builder()
        .dataSourceMetadata(dataSourceMetadata)
        .tags(getDefaultTagsForJobSubmission(dispatchQueryRequest))
        .queryId(queryIdProvider.getQueryId(dispatchQueryRequest));
  }

  private AsyncQueryHandler getQueryHandlerForFlintExtensionQuery(
      IndexQueryDetails indexQueryDetails) {
    if (isEligibleForIndexDMLHandling(indexQueryDetails)) {
      return queryHandlerFactory.getIndexDMLHandler();
    } else if (isEligibleForStreamingQuery(indexQueryDetails)) {
      return queryHandlerFactory.getStreamingQueryHandler();
    } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())) {
      // create should be handled by batch handler
      return queryHandlerFactory.getBatchQueryHandler();
    } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
      // manual refresh should be handled by batch handler
      return queryHandlerFactory.getRefreshQueryHandler();
    } else {
      return getDefaultAsyncQueryHandler();
    }
  }

  @NotNull
  private AsyncQueryHandler getDefaultAsyncQueryHandler() {
    return sessionManager.isEnabled()
        ? queryHandlerFactory.getInteractiveQueryHandler()
        : queryHandlerFactory.getBatchQueryHandler();
  }

  @NotNull
  private static IndexQueryDetails getIndexQueryDetails(DispatchQueryRequest dispatchQueryRequest) {
    IndexQueryDetails indexQueryDetails =
        SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
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
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata)
        .getQueryResponse(asyncQueryJobMetadata);
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata)
        .cancelJob(asyncQueryJobMetadata);
  }

  private AsyncQueryHandler getAsyncQueryHandlerForExistingQuery(
      AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return queryHandlerFactory.getInteractiveQueryHandler();
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      return queryHandlerFactory.getIndexDMLHandler();
    } else if (asyncQueryJobMetadata.getJobType() == JobType.BATCH) {
      return queryHandlerFactory.getRefreshQueryHandler();
    } else if (asyncQueryJobMetadata.getJobType() == JobType.STREAMING) {
      return queryHandlerFactory.getStreamingQueryHandler();
    } else {
      return queryHandlerFactory.getBatchQueryHandler();
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  private static final Logger LOG = LogManager.getLogger();
  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String CLUSTER_NAME_TAG_KEY = "domain_ident";
  public static final String JOB_TYPE_TAG_KEY = "type";

  private EMRServerlessClient emrServerlessClient;

  private DataSourceService dataSourceService;

  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private JobExecutionResponseReader jobExecutionResponseReader;

  private FlintIndexMetadataReader flintIndexMetadataReader;

  private Client client;

  private SessionManager sessionManager;

  private LeaseManager leaseManager;

  private StateStore stateStore;

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);

    AsyncQueryHandler asyncQueryHandler =
        sessionManager.isEnabled()
            ? new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager)
            : new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    DispatchQueryContext.DispatchQueryContextBuilder contextBuilder =
        DispatchQueryContext.builder()
            .dataSourceMetadata(dataSourceMetadata)
            .tags(getDefaultTagsForJobSubmission(dispatchQueryRequest))
            .queryId(AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()));

    // override asyncQueryHandler with specific.
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())
        && SQLQueryUtils.isFlintExtensionQuery(dispatchQueryRequest.getQuery())) {
      IndexQueryDetails indexQueryDetails =
          SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
      fillMissingDetails(dispatchQueryRequest, indexQueryDetails);
      contextBuilder.indexQueryDetails(indexQueryDetails);

      if (IndexQueryActionType.DROP.equals(indexQueryDetails.getIndexQueryActionType())) {
        asyncQueryHandler = createIndexDMLHandler();
      } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())
          && indexQueryDetails.isAutoRefresh()) {
        asyncQueryHandler =
            new StreamingQueryHandler(
                emrServerlessClient, jobExecutionResponseReader, leaseManager);
      } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
        // manual refresh should be handled by batch handler
        asyncQueryHandler =
            new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
      }
    }
    return asyncQueryHandler.submit(dispatchQueryRequest, contextBuilder.build());
  }

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager)
          .getQueryResponse(asyncQueryJobMetadata);
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      return createIndexDMLHandler().getQueryResponse(asyncQueryJobMetadata);
    } else {
      return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager)
          .getQueryResponse(asyncQueryJobMetadata);
    }
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    AsyncQueryHandler queryHandler;
    if (asyncQueryJobMetadata.getSessionId() != null) {
      queryHandler =
          new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager);
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      queryHandler = createIndexDMLHandler();
    } else {
      queryHandler =
          new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    }
    return queryHandler.cancelJob(asyncQueryJobMetadata);
  }

  private IndexDMLHandler createIndexDMLHandler() {
    return new IndexDMLHandler(
        emrServerlessClient,
        dataSourceService,
        dataSourceUserAuthorizationHelper,
        jobExecutionResponseReader,
        flintIndexMetadataReader,
        client,
        stateStore);
  }

  // TODO: Revisit this logic.
  // Currently, Spark if datasource is not provided in query.
  // Spark Assumes the datasource to be catalog.
  // This is required to handle drop index case properly when datasource name is not provided.
  private static void fillMissingDetails(
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SESSION_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
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
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
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
            : new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader);
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
        // todo, fix in DROP INDEX PR.
        return handleDropIndexQuery(dispatchQueryRequest, indexQueryDetails);
      } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())
          && indexQueryDetails.isAutoRefresh()) {
        asyncQueryHandler =
            new StreamingQueryHandler(emrServerlessClient, jobExecutionResponseReader);
      } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
        // manual refresh should be handled by batch handler
        asyncQueryHandler = new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader);
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
      return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader)
          .getQueryResponse(asyncQueryJobMetadata);
    }
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    AsyncQueryHandler queryHandler;
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager)
          .cancelJob(asyncQueryJobMetadata);
      queryHandler = new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader);
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      queryHandler = createIndexDMLHandler();
    } else {
      queryHandler = new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader);
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

  private DispatchQueryResponse handleDropIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(indexQueryDetails);
    // if index is created without auto refresh. there is no job to cancel.
    String status = JobRunState.FAILED.toString();
    try {
      if (indexMetadata.isAutoRefresh()) {
        emrServerlessClient.cancelJobRun(
            dispatchQueryRequest.getApplicationId(), indexMetadata.getJobId());
      }
    } finally {
      String indexName = indexQueryDetails.openSearchIndexName();
      try {
        AcknowledgedResponse response =
            client.admin().indices().delete(new DeleteIndexRequest().indices(indexName)).get();
        if (!response.isAcknowledged()) {
          LOG.error("failed to delete index");
        }
        status = JobRunState.SUCCESS.toString();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("failed to delete index");
      }
    }
    return new DispatchQueryResponse(
        AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()),
        new DropIndexResult(status).toJobId(),
        true,
        dataSourceMetadata.getResultIndex(),
        null);
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }
}

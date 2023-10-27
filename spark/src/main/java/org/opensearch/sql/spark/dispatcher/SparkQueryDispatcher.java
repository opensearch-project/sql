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
import java.util.Optional;
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
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.*;
import org.opensearch.sql.spark.execution.session.CreateSessionRequest;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
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

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      return handleSQLQuery(dispatchQueryRequest);
    } else {
      // Since we don't need any extra handling for PPL, we are treating it as normal dispatch
      // Query.
      return handleNonIndexQuery(dispatchQueryRequest);
    }
  }

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader)
          .getQueryResponse(asyncQueryJobMetadata);
    } else {
      return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader)
          .getQueryResponse(asyncQueryJobMetadata);
    }
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader)
          .cancelJob(asyncQueryJobMetadata);
    } else {
      return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader)
          .cancelJob(asyncQueryJobMetadata);
    }
  }

  private DispatchQueryResponse handleSQLQuery(DispatchQueryRequest dispatchQueryRequest) {
    if (SQLQueryUtils.isFlintExtensionQuery(dispatchQueryRequest.getQuery())) {
      IndexQueryDetails indexQueryDetails =
          SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
      fillMissingDetails(dispatchQueryRequest, indexQueryDetails);

      // TODO: refactor this code properly.
      if (IndexQueryActionType.DROP.equals(indexQueryDetails.getIndexQueryActionType())) {
        return handleDropIndexQuery(dispatchQueryRequest, indexQueryDetails);
      } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())) {
        return handleStreamingQueries(dispatchQueryRequest, indexQueryDetails);
      } else {
        return handleFlintNonStreamingQueries(dispatchQueryRequest, indexQueryDetails);
      }
    } else {
      return handleNonIndexQuery(dispatchQueryRequest);
    }
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

  private DispatchQueryResponse handleStreamingQueries(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    tags.put(INDEX_TAG_KEY, indexQueryDetails.openSearchIndexName());
    if (indexQueryDetails.isAutoRefresh()) {
      tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    }
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(
                    dataSourceService.getRawDataSourceMetadata(
                        dispatchQueryRequest.getDatasource()))
                .structuredStreaming(indexQueryDetails.isAutoRefresh())
                .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                .build()
                .toString(),
            tags,
            indexQueryDetails.isAutoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(
        AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()),
        jobId,
        false,
        dataSourceMetadata.getResultIndex(),
        null);
  }

  private DispatchQueryResponse handleFlintNonStreamingQueries(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(
                    dataSourceService.getRawDataSourceMetadata(
                        dispatchQueryRequest.getDatasource()))
                .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                .build()
                .toString(),
            tags,
            indexQueryDetails.isAutoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(
        AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()),
        jobId,
        false,
        dataSourceMetadata.getResultIndex(),
        null);
  }

  private DispatchQueryResponse handleNonIndexQuery(DispatchQueryRequest dispatchQueryRequest) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    AsyncQueryId queryId = AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "non-index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);

    if (sessionManager.isEnabled()) {
      Session session = null;

      if (dispatchQueryRequest.getSessionId() != null) {
        // get session from request
        SessionId sessionId = new SessionId(dispatchQueryRequest.getSessionId());
        Optional<Session> createdSession = sessionManager.getSession(sessionId);
        if (createdSession.isPresent()) {
          session = createdSession.get();
        }
      }
      if (session == null || !session.isReady()) {
        // create session if not exist or session dead/fail
        tags.put(JOB_TYPE_TAG_KEY, JobType.INTERACTIVE.getText());
        session =
            sessionManager.createSession(
                new CreateSessionRequest(
                    jobName,
                    dispatchQueryRequest.getApplicationId(),
                    dispatchQueryRequest.getExecutionRoleARN(),
                    SparkSubmitParameters.Builder.builder()
                        .className(FLINT_SESSION_CLASS_NAME)
                        .dataSource(
                            dataSourceService.getRawDataSourceMetadata(
                                dispatchQueryRequest.getDatasource()))
                        .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams()),
                    tags,
                    dataSourceMetadata.getResultIndex(),
                    dataSourceMetadata.getName()));
      }
      session.submit(
          new QueryRequest(
              queryId, dispatchQueryRequest.getLangType(), dispatchQueryRequest.getQuery()));
      return new DispatchQueryResponse(
          queryId,
          session.getSessionModel().getJobId(),
          false,
          dataSourceMetadata.getResultIndex(),
          session.getSessionId().getSessionId());
    } else {
      tags.put(JOB_TYPE_TAG_KEY, JobType.BATCH.getText());
      StartJobRequest startJobRequest =
          new StartJobRequest(
              dispatchQueryRequest.getQuery(),
              jobName,
              dispatchQueryRequest.getApplicationId(),
              dispatchQueryRequest.getExecutionRoleARN(),
              SparkSubmitParameters.Builder.builder()
                  .dataSource(
                      dataSourceService.getRawDataSourceMetadata(
                          dispatchQueryRequest.getDatasource()))
                  .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                  .build()
                  .toString(),
              tags,
              false,
              dataSourceMetadata.getResultIndex());
      String jobId = emrServerlessClient.startJobRun(startJobRequest);
      return new DispatchQueryResponse(
          queryId, jobId, false, dataSourceMetadata.getResultIndex(), null);
    }
  }

  private DispatchQueryResponse handleDropIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
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

  @Getter
  @RequiredArgsConstructor
  public static class DropIndexResult {
    private static final int PREFIX_LEN = 10;

    private final String status;

    public static DropIndexResult fromJobId(String jobId) {
      String status = new String(Base64.getDecoder().decode(jobId)).substring(PREFIX_LEN);
      return new DropIndexResult(status);
    }

    public String toJobId() {
      String queryId = RandomStringUtils.randomAlphanumeric(PREFIX_LEN) + status;
      return Base64.getEncoder().encodeToString(queryId.getBytes(StandardCharsets.UTF_8));
    }

    public JSONObject result() {
      JSONObject result = new JSONObject();
      if (JobRunState.SUCCESS.toString().equalsIgnoreCase(status)) {
        result.put(STATUS_FIELD, status);
        // todo. refactor response handling.
        JSONObject dummyData = new JSONObject();
        dummyData.put("result", new JSONArray());
        dummyData.put("schema", new JSONArray());
        dummyData.put("applicationId", "fakeDropIndexApplicationId");
        result.put(DATA_FIELD, dummyData);
      } else {
        result.put(STATUS_FIELD, status);
        result.put(ERROR_FIELD, "failed to drop index");
      }
      return result;
    }
  }
}

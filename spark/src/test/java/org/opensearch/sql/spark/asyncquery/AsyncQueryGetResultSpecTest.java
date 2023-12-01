/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;

import com.amazonaws.services.emrserverless.model.JobRunState;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class AsyncQueryGetResultSpecTest extends AsyncQueryExecutorServiceSpec {

  /** Mock Flint index and index state */
  private final FlintDatasetMock mockIndex =
      new FlintDatasetMock(
              "DROP SKIPPING INDEX ON mys3.default.http_logs",
              FlintIndexType.SKIPPING,
              "flint_mys3_default_http_logs_skipping_index")
          .latestId("skippingindexid");

  private MockFlintSparkJob mockIndexState;

  @Before
  public void doSetUp() {
    mockIndexState = new MockFlintSparkJob(mockIndex.latestId);
  }

  @Test
  public void testInteractiveQueryGetResult() {
    createAsyncQuery("SELECT 1")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(createResultDoc(interaction.queryId));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertQueryResults("SUCCESS", ImmutableList.of(tupleValue(Map.of("1", 1))));
  }

  @Test
  public void testInteractiveQueryGetResultWithConcurrentEmrJobUpdate() {
    createAsyncQuery("SELECT 1")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              JSONObject result = interaction.pluginSearchQueryResult();
              interaction.emrJobWriteResultDoc(createResultDoc(interaction.queryId));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return result;
            })
        .assertQueryResults("running", null)
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("SUCCESS", ImmutableList.of(tupleValue(Map.of("1", 1))));
  }

  @Test
  public void testBatchQueryGetResult() {
    createAsyncQuery("REFRESH SKIPPING INDEX ON test")
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(createEmptyResultDoc(interaction.queryId));
              interaction.emrJobUpdateJobState(JobRunState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertQueryResults("SUCCESS", ImmutableList.of());
  }

  @Test
  public void testBatchQueryGetResultWithConcurrentEmrJobUpdate() {
    createAsyncQuery("REFRESH SKIPPING INDEX ON test")
        .withInteraction(
            interaction -> {
              JSONObject result = interaction.pluginSearchQueryResult();
              interaction.emrJobWriteResultDoc(createEmptyResultDoc(interaction.queryId));
              interaction.emrJobUpdateJobState(JobRunState.SUCCESS);
              return result;
            })
        .assertQueryResults("running", null)
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("SUCCESS", ImmutableList.of());
  }

  @Test
  public void testStreamingQueryGetResult() {
    // Create mock index with index state refreshing
    mockIndex.createIndex();
    mockIndexState.refreshing();
    try {
      createAsyncQuery(
              "CREATE SKIPPING INDEX ON mys3.default.http_logs "
                  + "(l_orderkey VALUE_SET) WITH (auto_refresh = true)")
          .withInteraction(
              interaction -> {
                interaction.emrJobWriteResultDoc(createEmptyResultDoc(interaction.queryId));
                interaction.emrJobUpdateJobState(JobRunState.SUCCESS);
                return interaction.pluginSearchQueryResult();
              })
          .assertQueryResults("SUCCESS", ImmutableList.of());
    } finally {
      mockIndex.deleteIndex();
      mockIndexState.deleted();
    }
  }

  @Test
  public void testStreamingQueryGetResultWithConcurrentEmrJobUpdate() {
    // Create mock index with index state refreshing
    mockIndex.createIndex();
    mockIndexState.refreshing();
    try {
      createAsyncQuery(
              "CREATE SKIPPING INDEX ON mys3.default.http_logs "
                  + "(l_orderkey VALUE_SET) WITH (auto_refresh = true)")
          .withInteraction(
              interaction -> {
                JSONObject result = interaction.pluginSearchQueryResult();
                interaction.emrJobWriteResultDoc(createEmptyResultDoc(interaction.queryId));
                interaction.emrJobUpdateJobState(JobRunState.SUCCESS);
                return result;
              })
          .assertQueryResults("running", null)
          .withInteraction(InteractionStep::pluginSearchQueryResult)
          .assertQueryResults("SUCCESS", ImmutableList.of());
    } finally {
      mockIndex.deleteIndex();
      mockIndexState.deleted();
    }
  }

  @Test
  public void testDropIndexQueryGetResult() {
    // Create mock index with index state refreshing
    mockIndex.createIndex();
    mockIndexState.refreshing();

    LocalEMRSClient emrClient = new LocalEMRSClient();
    emrClient.setJobState(JobRunState.CANCELLED);
    createAsyncQuery(mockIndex.query, emrClient)
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("SUCCESS", ImmutableList.of());
  }

  @Test
  public void testDropIndexQueryGetResultWithResultDocRefreshDelay() {
    // Create mock index with index state refreshing
    mockIndex.createIndex();
    mockIndexState.refreshing();

    LocalEMRSClient emrClient = new LocalEMRSClient();
    emrClient.setJobState(JobRunState.CANCELLED);
    createAsyncQuery(mockIndex.query, emrClient)
        .withInteraction(interaction -> new JSONObject()) // simulate result index refresh delay
        .assertQueryResults("running", null)
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("SUCCESS", ImmutableList.of());
  }

  private AssertionHelper createAsyncQuery(String query) {
    return new AssertionHelper(query, new LocalEMRSClient());
  }

  private AssertionHelper createAsyncQuery(String query, LocalEMRSClient emrClient) {
    return new AssertionHelper(query, emrClient);
  }

  private class AssertionHelper {
    private final AsyncQueryExecutorService queryService;
    private final CreateAsyncQueryResponse createQueryResponse;
    private Interaction interaction;

    AssertionHelper(String query, LocalEMRSClient emrClient) {
      this.queryService =
          createAsyncQueryExecutorService(
              emrClient,
              /*
               * Custom reader that intercepts get results call and inject extra steps defined in
               * current interaction. Intercept both get methods for different query handler which
               * will only call either of them.
               */
              new JobExecutionResponseReader(client) {
                @Override
                public JSONObject getResultFromOpensearchIndex(String jobId, String resultIndex) {
                  return interaction.interact(new InteractionStep(emrClient, jobId, resultIndex));
                }

                @Override
                public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
                  return interaction.interact(new InteractionStep(emrClient, queryId, resultIndex));
                }
              });
      this.createQueryResponse =
          queryService.createAsyncQuery(
              new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null));
    }

    AssertionHelper withInteraction(Interaction interaction) {
      this.interaction = interaction;
      return this;
    }

    AssertionHelper assertQueryResults(String status, List<ExprValue> data) {
      AsyncQueryExecutionResponse results =
          queryService.getAsyncQueryResults(createQueryResponse.getQueryId());
      assertEquals(status, results.getStatus());
      assertEquals(data, results.getResults());
      return this;
    }
  }

  /** Define an interaction between PPL plugin and EMR-S job. */
  private interface Interaction {

    JSONObject interact(InteractionStep interaction);
  }

  /**
   * Each method in this class is one step that can happen in an interaction. These methods are
   * called in any order to simulate concurrent scenario.
   */
  private class InteractionStep {
    private final LocalEMRSClient emrClient;
    final String queryId;
    final String resultIndex;

    private InteractionStep(LocalEMRSClient emrClient, String queryId, String resultIndex) {
      this.emrClient = emrClient;
      this.queryId = queryId;
      this.resultIndex = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
    }

    /** Simulate PPL plugin search query_execution_result */
    JSONObject pluginSearchQueryResult() {
      return new JobExecutionResponseReader(client).getResultWithQueryId(queryId, resultIndex);
    }

    /** Simulate EMR-S bulk writes query_execution_result with refresh = wait_for */
    void emrJobWriteResultDoc(Map<String, Object> resultDoc) {
      try {
        IndexRequest request =
            new IndexRequest().index(resultIndex).setRefreshPolicy(WAIT_UNTIL).source(resultDoc);
        client.index(request).get();
      } catch (Exception e) {
        Assert.fail("Failed to write result doc: " + e.getMessage());
      }
    }

    /** Simulate EMR-S updates query_execution_request with state */
    void emrJobUpdateStatementState(StatementState newState) {
      StatementModel stmt = getStatement(stateStore, DATASOURCE).apply(queryId).get();
      StateStore.updateStatementState(stateStore, DATASOURCE).apply(stmt, newState);
    }

    void emrJobUpdateJobState(JobRunState jobState) {
      emrClient.setJobState(jobState);
    }
  }

  private Map<String, Object> createEmptyResultDoc(String queryId) {
    Map<String, Object> document = new HashMap<>();
    document.put("result", ImmutableList.of());
    document.put("schema", ImmutableList.of());
    document.put("jobRunId", "XXX");
    document.put("applicationId", "YYY");
    document.put("dataSourceName", DATASOURCE);
    document.put("status", "SUCCESS");
    document.put("error", "");
    document.put("queryId", queryId);
    document.put("queryText", "SELECT 1");
    document.put("sessionId", "ZZZ");
    document.put("updateTime", 1699124602715L);
    document.put("queryRunTime", 123);
    return document;
  }

  private Map<String, Object> createResultDoc(String queryId) {
    Map<String, Object> document = new HashMap<>();
    document.put("result", ImmutableList.of("{'1':1}"));
    document.put("schema", ImmutableList.of("{'column_name':'1','data_type':'integer'}"));
    document.put("jobRunId", "XXX");
    document.put("applicationId", "YYY");
    document.put("dataSourceName", DATASOURCE);
    document.put("status", "SUCCESS");
    document.put("error", "");
    document.put("queryId", queryId);
    document.put("queryText", "SELECT 1");
    document.put("sessionId", "ZZZ");
    document.put("updateTime", 1699124602715L);
    document.put("queryRunTime", 123);
    return document;
  }
}

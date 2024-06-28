/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;

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
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.response.OpenSearchJobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.transport.format.AsyncQueryResultResponseFormatter;
import org.opensearch.sql.spark.transport.model.AsyncQueryResult;

public class AsyncQueryGetResultSpecTest extends AsyncQueryExecutorServiceSpec {
  AsyncQueryRequestContext asyncQueryRequestContext = new NullAsyncQueryRequestContext();

  /** Mock Flint index and index state */
  private final FlintDatasetMock mockIndex =
      new FlintDatasetMock(
              "DROP SKIPPING INDEX ON mys3.default.http_logs",
              "REFRESH SKIPPING INDEX ON mys3.default.http_logs",
              FlintIndexType.SKIPPING,
              "flint_mys3_default_http_logs_skipping_index")
          .latestId("ZmxpbnRfbXlzM19kZWZhdWx0X2h0dHBfbG9nc19za2lwcGluZ19pbmRleA==");

  private MockFlintSparkJob mockIndexState;

  @Before
  public void doSetUp() {
    mockIndexState =
        new MockFlintSparkJob(flintIndexStateModelService, mockIndex.latestId, MYS3_DATASOURCE);
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

  @Test
  public void testInteractiveQueryResponse() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(createResultDoc(interaction.queryId));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"1\","
                + "\"type\":\"integer\"}],\"datarows\":[[1]],\"total\":1,\"size\":1}");
  }

  @Test
  public void testInteractiveQueryResponseBasicType() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{'column1': 'value1', 'column2': 123, 'column3': true}",
                          "{'column1': 'value2', 'column2': 456, 'column3': false}"),
                      ImmutableList.of(
                          "{'column_name': 'column1', 'data_type': 'string'}",
                          "{'column_name': 'column2', 'data_type': 'integer'}",
                          "{'column_name': 'column3', 'data_type': 'boolean'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"column1\",\"type\":\"string\"},{\"name\":\"column2\",\"type\":\"integer\"},{\"name\":\"column3\",\"type\":\"boolean\"}],\"datarows\":[[\"value1\",123,true],[\"value2\",456,false]],\"total\":2,\"size\":2}");
  }

  @Test
  public void testInteractiveQueryResponseJsonArray() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{ 'attributes': [{ 'key': 'telemetry.sdk.language', 'value': {"
                              + " 'stringValue': 'python' }}, { 'key': 'telemetry.sdk.name',"
                              + " 'value': { 'stringValue': 'opentelemetry' }}, { 'key':"
                              + " 'telemetry.sdk.version', 'value': { 'stringValue': '1.19.0' }}, {"
                              + " 'key': 'service.namespace', 'value': { 'stringValue':"
                              + " 'opentelemetry-demo' }}, { 'key': 'service.name', 'value': {"
                              + " 'stringValue': 'recommendationservice' }}, { 'key':"
                              + " 'telemetry.auto.version', 'value': { 'stringValue': '0.40b0'"
                              + " }}]}"),
                      ImmutableList.of("{'column_name':'attributes','data_type':'array'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"attributes\",\"type\":\"array\"}],\"datarows\":[[[{\"value\":{\"stringValue\":\"python\"},\"key\":\"telemetry.sdk.language\"},{\"value\":{\"stringValue\":\"opentelemetry\"},\"key\":\"telemetry.sdk.name\"},{\"value\":{\"stringValue\":\"1.19.0\"},\"key\":\"telemetry.sdk.version\"},{\"value\":{\"stringValue\":\"opentelemetry-demo\"},\"key\":\"service.namespace\"},{\"value\":{\"stringValue\":\"recommendationservice\"},\"key\":\"service.name\"},{\"value\":{\"stringValue\":\"0.40b0\"},\"key\":\"telemetry.auto.version\"}]]],\"total\":1,\"size\":1}");
  }

  @Test
  public void testInteractiveQueryResponseJsonNested() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{\n"
                              + "  'resourceSpans': {\n"
                              + "    'scopeSpans': {\n"
                              + "      'spans': {\n"
                              + "          'key': 'rpc.system',\n"
                              + "          'value': {\n"
                              + "            'stringValue': 'grpc'\n"
                              + "          }\n"
                              + "      }\n"
                              + "    }\n"
                              + "  }\n"
                              + "}"),
                      ImmutableList.of("{'column_name':'resourceSpans','data_type':'struct'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"resourceSpans\",\"type\":\"struct\"}],\"datarows\":[[{\"scopeSpans\":{\"spans\":{\"value\":{\"stringValue\":\"grpc\"},\"key\":\"rpc.system\"}}}]],\"total\":1,\"size\":1}");
  }

  @Test
  public void testInteractiveQueryResponseJsonNestedObjectArray() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{\n"
                              + "  'resourceSpans': \n"
                              + "    {\n"
                              + "      'scopeSpans': \n"
                              + "        {\n"
                              + "          'spans': \n"
                              + "            [\n"
                              + "              {\n"
                              + "                'attribute': {\n"
                              + "                  'key': 'rpc.system',\n"
                              + "                  'value': {\n"
                              + "                    'stringValue': 'grpc'\n"
                              + "                  }\n"
                              + "                }\n"
                              + "              },\n"
                              + "              {\n"
                              + "                'attribute': {\n"
                              + "                  'key': 'rpc.system',\n"
                              + "                  'value': {\n"
                              + "                    'stringValue': 'grpc'\n"
                              + "                  }\n"
                              + "                }\n"
                              + "              }\n"
                              + "            ]\n"
                              + "        }\n"
                              + "    }\n"
                              + "}"),
                      ImmutableList.of("{'column_name':'resourceSpans','data_type':'struct'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"resourceSpans\",\"type\":\"struct\"}],\"datarows\":[[{\"scopeSpans\":{\"spans\":[{\"attribute\":{\"value\":{\"stringValue\":\"grpc\"},\"key\":\"rpc.system\"}},{\"attribute\":{\"value\":{\"stringValue\":\"grpc\"},\"key\":\"rpc.system\"}}]}}]],\"total\":1,\"size\":1}");
  }

  @Test
  public void testExplainResponse() {
    createAsyncQuery("EXPLAIN SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of("{'plan':'== Physical Plan ==\\nAdaptiveSparkPlan'}"),
                      ImmutableList.of("{'column_name':'plan','data_type':'string'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"plan\",\"type\":\"string\"}],\"datarows\":[[\"=="
                + " Physical Plan ==\\n"
                + "AdaptiveSparkPlan\"]],\"total\":1,\"size\":1}");
  }

  @Test
  public void testInteractiveQueryEmptyResponseIssue2367() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{'srcPort':20641}",
                          "{'srcPort':20641}",
                          "{}",
                          "{}",
                          "{'srcPort':20641}",
                          "{'srcPort':20641}"),
                      ImmutableList.of("{'column_name':'srcPort','data_type':'long'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"srcPort\",\"type\":\"long\"}],\"datarows\":[[20641],[20641],[null],[null],[20641],[20641]],\"total\":6,\"size\":6}");
  }

  @Test
  public void testInteractiveQueryArrayResponseIssue2367() {
    createAsyncQuery("SELECT * FROM TABLE")
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("waiting", null)
        .withInteraction(
            interaction -> {
              interaction.emrJobWriteResultDoc(
                  createResultDoc(
                      interaction.queryId,
                      ImmutableList.of(
                          "{'resourceSpans':[{'resource':{'attributes':[{'key':'telemetry.sdk.language','value':{'stringValue':'python'}},{'key':'telemetry.sdk.name','value':{'stringValue':'opentelemetry'}}]},'scopeSpans':[{'scope':{'name':'opentelemetry.instrumentation.grpc','version':'0.40b0'},'spans':[{'attributes':[{'key':'rpc.system','value':{'stringValue':'grpc'}},{'key':'rpc.grpc.status_code','value':{'intValue':'0'}}],'kind':3},{'attributes':[{'key':'rpc.system','value':{'stringValue':'grpc'}},{'key':'rpc.grpc.status_code','value':{'intValue':'0'}}],'kind':3}]}]}]}"),
                      ImmutableList.of("{'column_name':'resourceSpans','data_type':'array'}")));
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return interaction.pluginSearchQueryResult();
            })
        .assertFormattedQueryResults(
            "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"resourceSpans\",\"type\":\"array\"}],\"datarows\":[[[{\"resource\":{\"attributes\":[{\"value\":{\"stringValue\":\"python\"},\"key\":\"telemetry.sdk.language\"},{\"value\":{\"stringValue\":\"opentelemetry\"},\"key\":\"telemetry.sdk.name\"}]},\"scopeSpans\":[{\"spans\":[{\"kind\":3,\"attributes\":[{\"value\":{\"stringValue\":\"grpc\"},\"key\":\"rpc.system\"},{\"value\":{\"intValue\":\"0\"},\"key\":\"rpc.grpc.status_code\"}]},{\"kind\":3,\"attributes\":[{\"value\":{\"stringValue\":\"grpc\"},\"key\":\"rpc.system\"},{\"value\":{\"intValue\":\"0\"},\"key\":\"rpc.grpc.status_code\"}]}],\"scope\":{\"name\":\"opentelemetry.instrumentation.grpc\",\"version\":\"0.40b0\"}}]}]]],\"total\":1,\"size\":1}");
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
      EMRServerlessClientFactory emrServerlessClientFactory = () -> emrClient;
      this.queryService =
          createAsyncQueryExecutorService(
              emrServerlessClientFactory,
              /*
               * Custom reader that intercepts get results call and inject extra steps defined in
               * current interaction. Intercept both get methods for different query handler which
               * will only call either of them.
               */
              new JobExecutionResponseReader() {
                @Override
                public JSONObject getResultWithJobId(String jobId, String resultIndex) {
                  return interaction.interact(new InteractionStep(emrClient, jobId, resultIndex));
                }

                @Override
                public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
                  return interaction.interact(new InteractionStep(emrClient, queryId, resultIndex));
                }
              });
      this.createQueryResponse =
          queryService.createAsyncQuery(
              new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
              asyncQueryRequestContext);
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

    AssertionHelper assertFormattedQueryResults(String expected) {
      AsyncQueryExecutionResponse results =
          queryService.getAsyncQueryResults(createQueryResponse.getQueryId());

      ResponseFormatter<AsyncQueryResult> formatter =
          new AsyncQueryResultResponseFormatter(JsonResponseFormatter.Style.COMPACT);
      assertEquals(
          expected,
          formatter.format(
              new AsyncQueryResult(
                  results.getStatus(),
                  results.getSchema(),
                  results.getResults(),
                  Cursor.None,
                  results.getError())));
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
      return new OpenSearchJobExecutionResponseReader(client)
          .getResultWithQueryId(queryId, resultIndex);
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
      StatementModel stmt = statementStorageService.getStatement(queryId, MYS3_DATASOURCE).get();
      statementStorageService.updateStatementState(stmt, newState);
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
    document.put("dataSourceName", MYS3_DATASOURCE);
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
    return createResultDoc(
        queryId,
        ImmutableList.of("{'1':1}"),
        ImmutableList.of("{'column_name" + "':'1','data_type':'integer'}"));
  }

  private Map<String, Object> createResultDoc(
      String queryId, List<String> result, List<String> schema) {
    Map<String, Object> document = new HashMap<>();
    document.put("result", result);
    document.put("schema", schema);
    document.put("jobRunId", "XXX");
    document.put("applicationId", "YYY");
    document.put("dataSourceName", MYS3_DATASOURCE);
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

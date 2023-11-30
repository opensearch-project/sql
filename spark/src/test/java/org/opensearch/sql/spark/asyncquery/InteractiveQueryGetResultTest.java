/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class InteractiveQueryGetResultTest extends AsyncQueryExecutorServiceSpec {

  @Test
  public void testSuccessfulStatementWithNullResult() {
    /*
    AsyncQueryExecutorService queryService =
        createAsyncQueryExecutorService(new LocalEMRSClient(), createJobExecutionResponseReader());
    CreateAsyncQueryResponse response =
        queryService.createAsyncQuery(
            new CreateAsyncQueryRequest("SELECT 1", DATASOURCE, LangType.SQL, null));
    AsyncQueryExecutionResponse results = queryService.getAsyncQueryResults(response.getQueryId());

    assertEquals(StatementState.RUNNING.getState(), results.getStatus());
    // assertEquals(List.of(integerValue(1)), results.getResults());
    */

    createAsyncQuery("SELECT 1")
        /*
        .withInteraction(
            new JobExecutionResponseReader(client) {
              @Override
              public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
                // 1) PPL plugin searches query_execution_result and return empty
                JSONObject result = super.getResultWithQueryId(queryId, resultIndex);

                // 2) EMR-S bulk writes query_execution_result with refresh = wait_for
                String resultIndexName = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
                try {
                  IndexRequest request =
                      new IndexRequest()
                          .index(resultIndexName)
                          .setRefreshPolicy(WAIT_UNTIL)
                          .source(createResultDoc(queryId));
                  client.index(request).get();
                } catch (Exception e) {
                  Assert.fail("Failed to write result doc: " + e.getMessage());
                }

                // 3) EMR-S updates query_execution_request with state=success
                StatementModel stmt = getStatement(stateStore, DATASOURCE).apply(queryId).get();
                updateStatementState(stateStore, DATASOURCE).apply(stmt, StatementState.SUCCESS);

                // 4) PPL plugin later reads query_execution_request and return state=success with
                // null
                // result
                return result;
              }
            })
         */
        .withInteraction(
            new PluginAndEmrJobInteraction() {
              @Override
              JSONObject interact(String queryId, String resultIndex) {
                JSONObject result = searchQueryResult(queryId, resultIndex);
                writeResultDoc(queryId, resultIndex);
                updateStatementState(queryId, StatementState.SUCCESS);
                return result;
              }
            })
        .assertQueryResults("running", null)
        .withInteraction(
            new PluginAndEmrJobInteraction() {
              @Override
              JSONObject interact(String queryId, String resultIndex) {
                return searchQueryResult(queryId, resultIndex);
              }
            })
        .assertQueryResults("SUCCESS", List.of(tupleValue(Map.of("1", 1))));
  }

  @Ignore
  public void testSuccessfulStatementWithResult() {
    AsyncQueryExecutorService queryService =
        createAsyncQueryExecutorService(new LocalEMRSClient(), createJobExecutionResponseReader());
    CreateAsyncQueryResponse response =
        queryService.createAsyncQuery(
            new CreateAsyncQueryRequest("SELECT 1", DATASOURCE, LangType.SQL, null));
    AsyncQueryExecutionResponse results = queryService.getAsyncQueryResults(response.getQueryId());

    assertEquals(StatementState.RUNNING.getState(), results.getStatus());
    // assertEquals(List.of(integerValue(1)), results.getResults());
  }

  private JobExecutionResponseReader createJobExecutionResponseReader() {
    // Reproduce the bug with the following steps executed by intercepting response reader
    return new JobExecutionResponseReader(client) {
      @Override
      public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
        // 1) PPL plugin searches query_execution_result and return empty
        JSONObject result = super.getResultWithQueryId(queryId, resultIndex);

        // 2) EMR-S bulk writes query_execution_result with refresh = wait_for
        String resultIndexName = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
        try {
          IndexRequest request =
              new IndexRequest()
                  .index(resultIndexName)
                  .setRefreshPolicy(WAIT_UNTIL)
                  .source(createResultDoc(queryId));
          client.index(request).get();
        } catch (Exception e) {
          Assert.fail("Failed to write result doc: " + e.getMessage());
        }

        // 3) EMR-S updates query_execution_request with state=success
        StatementModel stmt = getStatement(stateStore, DATASOURCE).apply(queryId).get();
        updateStatementState(stateStore, DATASOURCE).apply(stmt, StatementState.SUCCESS);

        // 4) PPL plugin later reads query_execution_request and return state=success with null
        // result
        return result;
      }
    };
  }

  private AssertionHelper createAsyncQuery(String query) {
    return new AssertionHelper(query);
  }

  private class AssertionHelper {
    private JobExecutionResponseReader responseReader;

    private final AsyncQueryExecutorService queryService;
    private final CreateAsyncQueryResponse createQueryResponse;

    private PluginAndEmrJobInteraction interaction;

    AssertionHelper(String query) {
      this.queryService =
          createAsyncQueryExecutorService(
              new LocalEMRSClient(),
              new JobExecutionResponseReader(client) {
                @Override
                public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
                  // Get results with current interaction
                  return interaction.interact(queryId, resultIndex);
                }
              });
      this.createQueryResponse =
          queryService.createAsyncQuery(
              new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null));
    }

    /*
    AssertionHelper withInteraction(JobExecutionResponseReader responseReader) {
      this.responseReader = responseReader;
      return this;
    }
    */

    AssertionHelper withInteraction(PluginAndEmrJobInteraction interaction) {
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

  private abstract class PluginAndEmrJobInteraction {

    abstract JSONObject interact(String queryId, String resultIndex);

    JSONObject searchQueryResult(String queryId, String resultIndex) {
      return new JobExecutionResponseReader(client).getResultWithQueryId(queryId, resultIndex);
    }

    void writeResultDoc(String queryId, String resultIndex) {
      // 2) EMR-S bulk writes query_execution_result with refresh = wait_for
      String resultIndexName = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
      try {
        IndexRequest request =
            new IndexRequest()
                .index(resultIndexName)
                .setRefreshPolicy(WAIT_UNTIL)
                .source(createResultDoc(queryId));
        client.index(request).get();
      } catch (Exception e) {
        Assert.fail("Failed to write result doc: " + e.getMessage());
      }
    }

    void updateStatementState(String queryId, StatementState newState) {
      // 3) EMR-S updates query_execution_request with state=success
      StatementModel stmt = getStatement(stateStore, DATASOURCE).apply(queryId).get();
      StateStore.updateStatementState(stateStore, DATASOURCE).apply(stmt, newState);
    }
  }

  private Map<String, Object> createResultDoc(String queryId) {
    Map<String, Object> document = new HashMap<>();
    document.put("result", new String[] {"{'1':1}"});
    document.put("schema", new String[] {"{'column_name':'1','data_type':'integer'}"});
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

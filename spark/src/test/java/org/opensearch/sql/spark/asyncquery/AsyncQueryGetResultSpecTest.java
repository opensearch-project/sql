/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Assert;
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

public class AsyncQueryGetResultSpecTest extends AsyncQueryExecutorServiceSpec {

  @Test
  public void testInteractiveQueryGetResult() {
    createAsyncQuery("SELECT 1")
        .withInteraction(
            interaction -> {
              JSONObject result = interaction.pluginSearchQueryResult();
              interaction.emrJobWriteResultDoc();
              interaction.emrJobUpdateStatementState(StatementState.SUCCESS);
              return result;
            })
        .assertQueryResults("running", null)
        .withInteraction(InteractionStep::pluginSearchQueryResult)
        .assertQueryResults("SUCCESS", List.of(tupleValue(Map.of("1", 1))));
  }

  private AssertionHelper createAsyncQuery(String query) {
    return new AssertionHelper(query);
  }

  private class AssertionHelper {
    private final AsyncQueryExecutorService queryService;
    private final CreateAsyncQueryResponse createQueryResponse;
    private Interaction interaction;

    AssertionHelper(String query) {
      this.queryService =
          createAsyncQueryExecutorService(
              new LocalEMRSClient(),
              new JobExecutionResponseReader(client) {
                @Override
                public JSONObject getResultWithQueryId(String queryId, String resultIndex) {
                  // Get results with extra steps defined in current interaction
                  return interaction.interact(new InteractionStep(queryId, resultIndex));
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
    private final String queryId;
    private final String resultIndex;

    private InteractionStep(String queryId, String resultIndex) {
      this.queryId = queryId;
      this.resultIndex = resultIndex == null ? DEFAULT_RESULT_INDEX : resultIndex;
    }

    /** Simulate PPL plugin search query_execution_result */
    JSONObject pluginSearchQueryResult() {
      return new JobExecutionResponseReader(client).getResultWithQueryId(queryId, resultIndex);
    }

    /** Simulate EMR-S bulk writes query_execution_result with refresh = wait_for */
    void emrJobWriteResultDoc() {
      try {
        IndexRequest request =
            new IndexRequest()
                .index(resultIndex)
                .setRefreshPolicy(WAIT_UNTIL)
                .source(createResultDoc(queryId));
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

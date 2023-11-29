/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

/** IT for bug fix in https://github.com/opensearch-project/sql/issues/2436. */
public class InteractiveQueryNullResultFixTest extends AsyncQueryExecutorServiceSpec {

  @Test
  public void reproduceSuccessStatementWithNullResult() {
    AsyncQueryExecutorService queryService = createAsyncQueryExecutorService(new LocalEMRSClient());
    CreateAsyncQueryResponse response =
        queryService.createAsyncQuery(
            new CreateAsyncQueryRequest("SELECT 1", DATASOURCE, LangType.SQL, null));
    AsyncQueryExecutionResponse results = queryService.getAsyncQueryResults(response.getQueryId());

    assertEquals(StatementState.SUCCESS.getState(), results.getStatus());
    assertNull(results.getResults());
  }

  @Override
  protected JobExecutionResponseReader createJobExecutionResponseReader() {
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

        // 4) PPL plugin reads query_execution_request and return state=success with null result
        return result;
      }
    };
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

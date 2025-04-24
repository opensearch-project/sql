/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SQLServiceTest {

  private static String QUERY = "/_plugins/_sql";

  private static String EXPLAIN = "/_plugins/_sql/_explain";

  private SQLService sqlService;

  private DefaultQueryManager queryManager;

  @Mock private QueryService queryService;

  @BeforeEach
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();
    sqlService =
        new SQLService(new SQLSyntaxParser(), queryManager, new QueryPlanFactory(queryService));
  }

  @AfterEach
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  private ResponseListener<QueryResponse> getQueryListener(boolean fail) {
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        if (fail) {
          fail();
        } else {
          assertNotNull(response);
        }
      }

      @Override
      public void onFailure(Exception e) {
        if (!fail) {
          fail(e);
        } else {
          assertNotNull(e);
        }
      }
    };
  }

  private ResponseListener<ExplainResponse> getExplainListener(boolean fail) {
    return new ResponseListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse response) {
        if (fail) {
          fail();
        } else {
          assertNotNull(response);
        }
      }

      @Override
      public void onFailure(Exception e) {
        if (!fail) {
          fail(e);
        }
      }
    };
  }

  @Test
  public void can_execute_sql_query() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "jdbc"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_execute_cursor_query() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), null, QUERY, Map.of("format", "jdbc"), "n:cursor"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_execute_close_cursor_query() {
    sqlService.execute(
        new SQLQueryRequest(
            new JSONObject(), null, QUERY + "/close", Map.of("format", "jdbc"), "n:cursor"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_execute_csv_format_request() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "csv"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_execute_raw_format_request() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "raw"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_execute_pretty_raw_format_request() {
    sqlService.execute(
        new SQLQueryRequest(
            new JSONObject(),
            "SELECT 123",
            QUERY,
            Map.of("format", "jdbc", "pretty", "true"),
            "n:cursor"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void can_explain_sql_query() {
    doAnswer(
            invocation -> {
              ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
              listener.onResponse(new ExplainResponse(new ExplainResponseNode("Test")));
              return null;
            })
        .when(queryService)
        .explain(any(), any(), any(), any());

    sqlService.explain(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", EXPLAIN, "csv"),
        getExplainListener(false));
  }

  @Test
  public void cannot_explain_cursor_query() {
    sqlService.explain(
        new SQLQueryRequest(new JSONObject(), null, EXPLAIN, Map.of("format", "jdbc"), "n:cursor"),
        getExplainListener(true));
  }

  @Test
  public void can_capture_error_during_execution() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT", QUERY, ""),
        getQueryListener(true),
        getExplainListener(false));
  }

  @Test
  public void can_capture_error_during_explain() {
    sqlService.explain(
        new SQLQueryRequest(new JSONObject(), "SELECT", EXPLAIN, ""), getExplainListener(true));
  }
}

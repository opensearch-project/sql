/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import java.util.List;
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

  @Mock
  private QueryService queryService;

  @BeforeEach
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();
    sqlService = new SQLService(new SQLSyntaxParser(), queryManager,
        new QueryPlanFactory(queryService));
  }

  @AfterEach
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void can_execute_sql_query() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "jdbc"),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void can_execute_cursor_query() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), null, QUERY, Map.of("format", "jdbc"), "n:cursor", List.of()),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void can_execute_close_cursor_query() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), null, QUERY + "/close",
            Map.of("format", "jdbc"), "n:cursor", List.of()),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void can_execute_csv_format_request() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "csv"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void can_explain_sql_query() {
    doAnswer(invocation -> {
      ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
      listener.onResponse(new ExplainResponse(new ExplainResponseNode("Test")));
      return null;
    }).when(queryService).explain(any(), any());

    sqlService.explain(new SQLQueryRequest(new JSONObject(), "SELECT 123", EXPLAIN, "csv"),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void cannot_explain_cursor_query() {
    sqlService.explain(new SQLQueryRequest(new JSONObject(), null, EXPLAIN,
            Map.of("format", "jdbc"), "n:cursor", List.of()),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            fail(response.toString());
          }

          @Override
          public void onFailure(Exception e) {
            assertEquals("Explain of a paged query continuation is not supported."
                + " Use `explain` for the initial query request.", e.getMessage());
          }
        });
  }

  @Test
  public void can_capture_error_during_execution() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT", QUERY, ""),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertNotNull(e);
          }
        });
  }

  @Test
  public void can_capture_error_during_explain() {
    sqlService.explain(
        new SQLQueryRequest(new JSONObject(), "SELECT", EXPLAIN, ""),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            fail("Should fail as expected");
          }

          @Override
          public void onFailure(Exception e) {
            assertNotNull(e);
          }
        });
  }
}

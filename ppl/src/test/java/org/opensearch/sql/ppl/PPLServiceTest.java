/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

@RunWith(MockitoJUnitRunner.class)
public class PPLServiceTest {

  private static final String QUERY = "/_plugins/_ppl";

  private static final String EXPLAIN = "/_plugins/_ppl/_explain";

  private PPLService pplService;

  private DefaultQueryManager queryManager;

  @Mock private QueryService queryService;

  @Mock private ExecutionEngine.Schema schema;

  @Mock private Settings settings;

  /** Setup the test context. */
  @Before
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();

    pplService =
        new PPLService(
            new PPLSyntaxParser(), queryManager, new QueryPlanFactory(queryService), settings);
  }

  @After
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  private ResponseListener<QueryResponse> getQueryListener(boolean fail) {
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        if (fail) {
          Assert.fail();
        }
      }

      @Override
      public void onFailure(Exception e) {
        if (!fail) {
          Assert.fail();
        }
      }
    };
  }

  /**
   * Tolerant of either outcome: these tests assert on the query-insights sink, which is fed before
   * the analyze plan is submitted, and the plan itself runs against a mocked query service.
   */
  private ResponseListener<AnalyzeResponse> getAnalyzeListener() {
    return new ResponseListener<AnalyzeResponse>() {
      @Override
      public void onResponse(AnalyzeResponse response) {}

      @Override
      public void onFailure(Exception e) {}
    };
  }

  private ResponseListener<ExplainResponse> getExplainListener(boolean fail) {
    return new ResponseListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse response) {
        if (fail) {
          Assert.fail();
        }
      }

      @Override
      public void onFailure(Exception e) {
        if (!fail) {
          Assert.fail();
        }
      }
    };
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(4);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any(), any(), anyBoolean(), any());

    pplService.execute(
        new PPLQueryRequest("search source=t a=1", null, QUERY),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void testExecutePassesAnonymizedQueryToSink() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(4);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any(), any(), anyBoolean(), any());

    AtomicReference<QueryInsightsMetadata> metadata = new AtomicReference<>();
    pplService.execute(
        new PPLQueryRequest("search source=t a=42", null, QUERY),
        getQueryListener(false),
        getExplainListener(false),
        metadata::set);

    // The sink receives the anonymized query (literal masked, never the raw value) and the source
    // index resolved from the same AST that executes.
    Assert.assertNotNull(metadata.get());
    Assert.assertTrue(metadata.get().anonymizedQuery().contains("***"));
    Assert.assertFalse(metadata.get().anonymizedQuery().contains("42"));
    Assert.assertEquals(Collections.singletonList("t"), metadata.get().indices());
  }

  @Test
  public void testExplainPassesAnonymizedQueryToSink() {
    AtomicReference<QueryInsightsMetadata> metadata = new AtomicReference<>();
    pplService.explain(
        new PPLQueryRequest("search source=t a=42", null, EXPLAIN),
        getExplainListener(false),
        metadata::set);

    Assert.assertNotNull(metadata.get());
    Assert.assertTrue(metadata.get().anonymizedQuery().contains("***"));
    Assert.assertFalse(metadata.get().anonymizedQuery().contains("42"));
    Assert.assertEquals(Collections.singletonList("t"), metadata.get().indices());
  }

  @Test
  public void testAnalyzePassesAnonymizedQueryToSink() {
    AtomicReference<QueryInsightsMetadata> metadata = new AtomicReference<>();
    pplService.analyze(
        new PPLQueryRequest("search source=t a=42", null, QUERY),
        getAnalyzeListener(),
        metadata::set);

    // analyze builds the AST on its own path rather than sharing execute's, so the sink has to be
    // fed there too; without this the analyze route would report an empty record.
    Assert.assertNotNull(metadata.get());
    Assert.assertTrue(metadata.get().anonymizedQuery().contains("***"));
    Assert.assertFalse(metadata.get().anonymizedQuery().contains("42"));
    Assert.assertEquals(Collections.singletonList("t"), metadata.get().indices());
    Assert.assertFalse(metadata.get().explain());
  }

  @Test
  public void testAnalyzeWithoutSinkShouldPass() {
    // The two-arg overload delegates with the no-op sink; it must not throw.
    pplService.analyze(
        new PPLQueryRequest("search source=t a=42", null, QUERY), getAnalyzeListener());
  }

  @Test
  public void testAnalyzeWithIllegalQueryShouldBeCaughtByHandler() {
    AtomicReference<Exception> failure = new AtomicReference<>();
    AtomicReference<QueryInsightsMetadata> metadata = new AtomicReference<>();
    pplService.analyze(
        new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<AnalyzeResponse>() {
          @Override
          public void onResponse(AnalyzeResponse response) {
            Assert.fail("a query that fails to parse must not produce an analyze response");
          }

          @Override
          public void onFailure(Exception e) {
            failure.set(e);
          }
        },
        metadata::set);

    // The parse failure must reach the listener rather than escape analyze(), and the sink must
    // stay untouched: there is no anonymized query to report for a query that never parsed.
    Assert.assertNotNull(failure.get());
    Assert.assertNull(metadata.get());
  }

  @Test
  public void testExecuteCsvFormatShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(4);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any(), any(), anyBoolean(), any());

    pplService.execute(
        new PPLQueryRequest("search source=t a=1", null, QUERY, "csv"),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void testExplainShouldPass() {
    pplService.explain(
        new PPLQueryRequest("search source=t a=1", null, EXPLAIN),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {}

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.execute(
        new PPLQueryRequest("search", null, QUERY),
        getQueryListener(true),
        getExplainListener(false));
  }

  @Test
  public void testExplainWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.explain(new PPLQueryRequest("search", null, QUERY), getExplainListener(true));
  }

  @Test
  public void testPrometheusQuery() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(4);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any(), any(), anyBoolean(), any());

    pplService.execute(
        new PPLQueryRequest("source = prometheus.http_requests_total", null, QUERY),
        getQueryListener(false),
        getExplainListener(false));
  }

  @Test
  public void testInvalidPPLQuery() {
    pplService.execute(
        new PPLQueryRequest("search", null, QUERY),
        getQueryListener(true),
        getExplainListener(false));
  }
}

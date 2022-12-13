/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.ppl.config.PPLServiceConfig;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@RunWith(MockitoJUnitRunner.class)
public class PPLServiceTest {

  private static String QUERY = "/_plugins/_ppl";

  private static String EXPLAIN = "/_plugins/_ppl/_explain";

  private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  private PPLService pplService;

  @Mock
  private QueryService queryService;

  @Mock
  private StorageEngine storageEngine;

  @Mock
  private ExecutionEngine executionEngine;

  @Mock
  private DataSourceService dataSourceService;

  @Mock
  private ExecutionEngine.Schema schema;

  private DefaultQueryManager queryManager;

  /**
   * Setup the test context.
   */
  @Before
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();
    context.registerBean(QueryManager.class, () -> queryManager);
    context.registerBean(QueryPlanFactory.class, () -> new QueryPlanFactory(queryService));
    context.registerBean(StorageEngine.class, () -> storageEngine);
    context.registerBean(ExecutionEngine.class, () -> executionEngine);
    context.registerBean(DataSourceService.class, () -> dataSourceService);
    context.registerBean(FunctionProperties.class, FunctionProperties::new);
    context.register(PPLServiceConfig.class);
    context.refresh();
    pplService = context.getBean(PPLService.class);
  }

  @After
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryService).execute(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {

          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteCsvFormatShouldPass() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryService).execute(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, QUERY, "csv"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExplainShouldPass() {
    doAnswer(invocation -> {
      ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
      listener.onResponse(new ExplainResponse(new ExplainResponseNode("test")));
      return null;
    }).when(queryService).explain(any(), any());

    pplService.explain(new PPLQueryRequest("search source=t a=1", null, EXPLAIN),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {
          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.execute(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }

  @Test
  public void testExplainWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.explain(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }

  @Test
  public void testPrometheusQuery() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryService).execute(any(), any());

    pplService.execute(new PPLQueryRequest("source = prometheus.http_requests_total", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {

          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testInvalidPPLQuery() {
    pplService.execute(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }
}

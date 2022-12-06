/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.defaultOpenSearchDataSourceMetadata;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.monitor.AlwaysHealthyMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.ppl.config.PPLServiceConfig;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.util.ExecuteOnCallerThreadQueryManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Run PPL with query engine outside OpenSearch cluster. This IT doesn't require our plugin
 * installed actually. The client application, ex. JDBC driver, needs to initialize all components
 * itself required by ppl service.
 */
public class StandaloneIT extends PPLIntegTestCase {

  private RestHighLevelClient restClient;

  private PPLService pplService;

  @Override
  public void init() {
    // Using client() defined in ODFERestTestCase.
    restClient = new InternalRestHighLevelClient(client());

    OpenSearchClient client = new OpenSearchRestClient(restClient);
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.registerBean(ExecutionEngine.class, () -> new OpenSearchExecutionEngine(client,
        new OpenSearchExecutionProtector(new AlwaysHealthyMonitor())));
    context.registerBean(OpenSearchClient.class, () -> client);
    context.registerBean(Settings.class, () -> defaultSettings());
    context.registerBean(FunctionProperties.class, FunctionProperties::new);
    DataSourceService dataSourceService = new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new OpenSearchDataSourceFactory(client, defaultSettings()))
            .build());
    dataSourceService.addDataSource(defaultOpenSearchDataSourceMetadata());
    context.registerBean(DataSourceService.class, () -> dataSourceService);
    context.register(StandaloneConfig.class);
    context.register(PPLServiceConfig.class);
    context.refresh();

    pplService = context.getBean(PPLService.class);
  }

  @Test
  public void testSourceFieldQuery() throws IOException {
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    String actual = executeByStandaloneQueryEngine("source=test | fields name");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  private String executeByStandaloneQueryEngine(String query) {
    AtomicReference<String> actual = new AtomicReference<>();
    pplService.execute(
        new PPLQueryRequest(query, null, null),
        new ResponseListener<QueryResponse>() {

          @Override
          public void onResponse(QueryResponse response) {
            QueryResult result = new QueryResult(response.getSchema(), response.getResults());
            String json = new SimpleJsonResponseFormatter(PRETTY).format(result);
            actual.set(json);
          }

          @Override
          public void onFailure(Exception e) {
            throw new IllegalStateException("Exception happened during execution", e);
          }
        });
    return actual.get();
  }

  private Settings defaultSettings() {
    return new Settings() {
      private final Map<Key, Integer> defaultSettings = new ImmutableMap.Builder<Key, Integer>()
          .put(Key.QUERY_SIZE_LIMIT, 200)
          .build();

      @Override
      public <T> T getSettingValue(Key key) {
        return (T) defaultSettings.get(key);
      }

      @Override
      public List<?> getSettings() {
        return (List<?>) defaultSettings;
      }
    };
  }

  /**
   * Internal RestHighLevelClient only for testing purpose.
   */
  static class InternalRestHighLevelClient extends RestHighLevelClient {
    public InternalRestHighLevelClient(RestClient restClient) {
      super(restClient, RestClient::close, Collections.emptyList());
    }
  }

  @Configuration
  static class StandaloneConfig {
    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private ExecutionEngine executionEngine;

    @Bean
    QueryManager queryManager() {
      return new ExecuteOnCallerThreadQueryManager();
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    QueryPlanFactory queryExecutionFactory() {
      BuiltinFunctionRepository functionRepository = BuiltinFunctionRepository.getInstance();
      Analyzer analyzer = new Analyzer(new ExpressionAnalyzer(functionRepository),
          dataSourceService, functionRepository);
      Planner planner =
          new Planner(LogicalPlanOptimizer.create());
      return new QueryPlanFactory(new QueryService(analyzer, executionEngine, planner));
    }
  }
}

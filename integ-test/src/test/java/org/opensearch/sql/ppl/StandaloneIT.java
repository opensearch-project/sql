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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelper;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.monitor.AlwaysHealthyMonitor;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.util.ExecuteOnCallerThreadQueryManager;

/**
 * Run PPL with query engine outside OpenSearch cluster. This IT doesn't require our plugin
 * installed actually. The client application, ex. JDBC driver, needs to initialize all components
 * itself required by ppl service.
 */
public class StandaloneIT extends PPLIntegTestCase {

  private PPLService pplService;

  @Override
  public void init() {
    RestHighLevelClient restClient = new InternalRestHighLevelClient(client());
    OpenSearchClient client = new OpenSearchRestClient(restClient);
    DataSourceService dataSourceService = new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new OpenSearchDataSourceFactory(client, defaultSettings()))
            .build(), getDataSourceMetadataStorage(), getDataSourceUserRoleHelper());
    dataSourceService.createDataSource(defaultOpenSearchDataSourceMetadata());

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new StandaloneModule(new InternalRestHighLevelClient(client()), defaultSettings(), dataSourceService));
    Injector injector = modules.createInjector();
    pplService =
        SecurityAccess.doPrivileged(() -> injector.getInstance(PPLService.class));
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

  @RequiredArgsConstructor
  public class StandaloneModule extends AbstractModule {

    private final RestHighLevelClient client;

    private final Settings settings;

    private final DataSourceService dataSourceService;

    private final BuiltinFunctionRepository functionRepository =
        BuiltinFunctionRepository.getInstance(null);

    @Override
    protected void configure() {}

    @Provides
    public OpenSearchClient openSearchClient() {
      return new OpenSearchRestClient(client);
    }

    @Provides
    public StorageEngine storageEngine(OpenSearchClient client) {
      return new OpenSearchStorageEngine(client, settings);
    }

    @Provides
    public ExecutionEngine executionEngine(OpenSearchClient client, ExecutionProtector protector,
                                           PlanSerializer planSerializer) {
      return new OpenSearchExecutionEngine(client, protector, planSerializer);
    }

    @Provides
    public ResourceMonitor resourceMonitor() {
      return new AlwaysHealthyMonitor();
    }

    @Provides
    public ExecutionProtector protector(ResourceMonitor resourceMonitor) {
      return new OpenSearchExecutionProtector(resourceMonitor);
    }

    @Provides
    @Singleton
    public QueryManager queryManager() {
      return new ExecuteOnCallerThreadQueryManager();
    }

    @Provides
    public PPLService pplService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
      return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory);
    }

    @Provides
    public SQLService sqlService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
      return new SQLService(new SQLSyntaxParser(), queryManager, queryPlanFactory);
    }

    @Provides
    public PlanSerializer planSerializer(StorageEngine storageEngine) {
      return new PlanSerializer(storageEngine);
    }

    @Provides
    public QueryPlanFactory queryPlanFactory(ExecutionEngine executionEngine) {
      Analyzer analyzer =
          new Analyzer(
              new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
      Planner planner = new Planner(LogicalPlanOptimizer.create());
      QueryService queryService = new QueryService(analyzer, executionEngine, planner);
      return new QueryPlanFactory(queryService);
    }
  }

  public static DataSourceMetadataStorage getDataSourceMetadataStorage() {
    return new DataSourceMetadataStorage() {
      @Override
      public List<DataSourceMetadata> getDataSourceMetadata() {
        return Collections.emptyList();
      }

      @Override
      public Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName) {
        return Optional.empty();
      }

      @Override
      public void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {

      }

      @Override
      public void updateDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {

      }

      @Override
      public void deleteDataSourceMetadata(String datasourceName) {

      }
    };
  }

  public static DataSourceUserAuthorizationHelper getDataSourceUserRoleHelper() {
    return new DataSourceUserAuthorizationHelper() {
      @Override
      public void authorizeDataSource(DataSourceMetadata dataSourceMetadata) {

      }
    };
  }
}

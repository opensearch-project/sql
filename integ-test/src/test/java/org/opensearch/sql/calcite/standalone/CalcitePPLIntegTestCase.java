/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

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
import org.json.JSONObject;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelper;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.UnsupportedCursorRequestException;
import org.opensearch.sql.executor.ExecutionEngine;
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
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.util.ExecuteOnCallerThreadQueryManager;

/**
 * This abstract test case provide a standalone env to run PPL query, IT extends this class could
 * debug the service side execution of PPL in IDE.
 */
public abstract class CalcitePPLIntegTestCase extends PPLIntegTestCase {
  protected PPLService pplService;

  @Override
  public void init() throws IOException {
    RestHighLevelClient restClient =
        new CalcitePPLIntegTestCase.InternalRestHighLevelClient(client());
    OpenSearchClient client = new OpenSearchRestClient(restClient);
    DataSourceService dataSourceService =
        new DataSourceServiceImpl(
            new ImmutableSet.Builder<DataSourceFactory>()
                .add(new OpenSearchDataSourceFactory(client, getSettings()))
                .build(),
            getDataSourceMetadataStorage(),
            getDataSourceUserRoleHelper());
    dataSourceService.createDataSource(defaultOpenSearchDataSourceMetadata());

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        new CalcitePPLIntegTestCase.StandaloneModule(
            new CalcitePPLIntegTestCase.InternalRestHighLevelClient(client()),
            getSettings(),
            dataSourceService));
    Injector injector = modules.createInjector();
    pplService = SecurityAccess.doPrivileged(() -> injector.getInstance(PPLService.class));
  }

  protected Settings getSettings() {
    return defaultSettings();
  }

  private Settings defaultSettings() {
    System.out.println(Settings.Key.CALCITE_PUSHDOWN_ENABLED.name() + " disabled");
    return new Settings() {
      private final Map<Key, Object> defaultSettings =
          new ImmutableMap.Builder<Key, Object>()
              .put(Key.QUERY_SIZE_LIMIT, 200)
              .put(Key.SQL_PAGINATION_API_SEARCH_AFTER, true)
              .put(Key.FIELD_TYPE_TOLERANCE, true)
              .put(Key.CALCITE_ENGINE_ENABLED, true)
              .put(Key.CALCITE_FALLBACK_ALLOWED, false)
              .put(Key.CALCITE_PUSHDOWN_ENABLED, false)
              .put(Key.DEFAULT_PATTERN_METHOD, "SIMPLE_PATTERN")
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

  protected Settings enablePushdown() {
    System.out.println(Settings.Key.CALCITE_PUSHDOWN_ENABLED.name() + " enabled");
    return new Settings() {
      private final Map<Key, Object> defaultSettings =
          new ImmutableMap.Builder<Key, Object>()
              .put(Key.QUERY_SIZE_LIMIT, 200)
              .put(Key.SQL_PAGINATION_API_SEARCH_AFTER, true)
              .put(Key.FIELD_TYPE_TOLERANCE, true)
              .put(Key.CALCITE_ENGINE_ENABLED, true)
              .put(Key.CALCITE_FALLBACK_ALLOWED, false)
              .put(Key.CALCITE_PUSHDOWN_ENABLED, true)
              .put(Key.DEFAULT_PATTERN_METHOD, "SIMPLE_PATTERN")
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

  public boolean isPushdownEnabled() {
    return getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED);
  }

  protected String execute(String query) {
    AtomicReference<String> actual = new AtomicReference<>();
    pplService.execute(
        new PPLQueryRequest(query, null, null),
        new ResponseListener<ExecutionEngine.QueryResponse>() {

          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            QueryResult result = new QueryResult(response.getSchema(), response.getResults());
            String json = new SimpleJsonResponseFormatter(PRETTY).format(result);
            actual.set(json);
          }

          @Override
          public void onFailure(Exception e) {
            throw new IllegalStateException("Exception happened during execution", e);
          }
        },
        new ResponseListener<ExecutionEngine.ExplainResponse>() {

          @Override
          public void onResponse(ExecutionEngine.ExplainResponse response) {
            actual.set(response.getCalcite().toString());
          }

          @Override
          public void onFailure(Exception e) {
            throw new IllegalStateException("Exception happened during execution", e);
          }
        });
    return actual.get();
  }

  @Override
  protected JSONObject executeQuery(String query) {
    AtomicReference<JSONObject> actual = new AtomicReference<>();
    pplService.execute(
        new PPLQueryRequest(query, null, null),
        new ResponseListener<ExecutionEngine.QueryResponse>() {

          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            QueryResult result = new QueryResult(response.getSchema(), response.getResults());
            String json = new SimpleJsonResponseFormatter(PRETTY).format(result);
            actual.set(jsonify(json));
          }

          @Override
          public void onFailure(Exception e) {
            if (e instanceof SyntaxCheckException) {
              throw (SyntaxCheckException) e;
            } else if (e instanceof QueryEngineException) {
              throw (QueryEngineException) e;
            } else if (e instanceof UnsupportedCursorRequestException) {
              throw (UnsupportedCursorRequestException) e;
            } else if (e instanceof NoCursorException) {
              throw (NoCursorException) e;
            } else if (e instanceof UnsupportedOperationException) {
              throw (UnsupportedOperationException) e;
            } else if (e instanceof IllegalArgumentException) {
              // most exceptions thrown by Calcite when resolve a plan.
              throw (IllegalArgumentException) e;
            } else {
              throw new IllegalStateException("Exception happened during execution", e);
            }
          }
        },
        new ResponseListener<ExecutionEngine.ExplainResponse>() {

          @Override
          public void onResponse(ExecutionEngine.ExplainResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail();
          }
        });
    return actual.get();
  }

  @Override
  protected String explainQuery(String query) {
    AtomicReference<String> actual = new AtomicReference<>();
    pplService.explain(
        new PPLQueryRequest(query, null, null),
        new ResponseListener<ExecutionEngine.ExplainResponse>() {

          @Override
          public void onResponse(ExecutionEngine.ExplainResponse response) {
            String responseContent =
                new JsonResponseFormatter<ExecutionEngine.ExplainResponse>(PRETTY) {
                  @Override
                  protected Object buildJsonObject(ExecutionEngine.ExplainResponse response) {
                    return response;
                  }
                }.format(response);
            actual.set(responseContent.replace("\\r\\n", "\\n"));
          }

          @Override
          public void onFailure(Exception e) {
            throw new IllegalStateException("Exception happened during execution", e);
          }
        });
    return actual.get();
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
      public void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {}

      @Override
      public void updateDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {}

      @Override
      public void deleteDataSourceMetadata(String datasourceName) {}
    };
  }

  public static DataSourceUserAuthorizationHelper getDataSourceUserRoleHelper() {
    return new DataSourceUserAuthorizationHelper() {
      @Override
      public void authorizeDataSource(DataSourceMetadata dataSourceMetadata) {}
    };
  }

  /** Internal RestHighLevelClient only for testing purpose. */
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
        BuiltinFunctionRepository.getInstance();

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
    public ExecutionEngine executionEngine(
        OpenSearchClient client, ExecutionProtector protector, PlanSerializer planSerializer) {
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
      return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory, settings);
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
      QueryService queryService =
          new QueryService(analyzer, executionEngine, planner, dataSourceService, settings);
      return new QueryPlanFactory(queryService);
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.defaultOpenSearchDataSourceMetadata;
import static org.opensearch.sql.executor.QueryType.SQL;
import static org.opensearch.sql.ppl.StandaloneIT.getDataSourceMetadataStorage;
import static org.opensearch.sql.ppl.StandaloneIT.getDataSourceUserRoleHelper;
import static org.opensearch.sql.util.Capability.PAGINATION_CURSOR;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.util.InternalRestHighLevelClient;
import org.opensearch.sql.util.RequiresCapability;
import org.opensearch.sql.util.StandaloneModule;
import org.opensearch.sql.utils.DeserializationFilterUtil;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@RequiresCapability(PAGINATION_CURSOR)
public class StandalonePaginationIT extends SQLIntegTestCase {

  private QueryService queryService;

  private PlanSerializer planSerializer;

  private OpenSearchClient client;

  @Override
  @SneakyThrows
  public void init() {
    RestHighLevelClient restClient = new InternalRestHighLevelClient(client());
    client = new OpenSearchRestClient(restClient);
    DataSourceService dataSourceService =
        new DataSourceServiceImpl(
            new ImmutableSet.Builder<DataSourceFactory>()
                .add(new OpenSearchDataSourceFactory(client, defaultSettings()))
                .build(),
            getDataSourceMetadataStorage(),
            getDataSourceUserRoleHelper());
    dataSourceService.createDataSource(defaultOpenSearchDataSourceMetadata());

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        new StandaloneModule(
            new InternalRestHighLevelClient(client()), defaultSettings(), dataSourceService));
    Injector injector = modules.createInjector();

    queryService = injector.getInstance(QueryService.class);
    planSerializer = injector.getInstance(PlanSerializer.class);
  }

  @Test
  public void test_pagination_whitebox() throws IOException {
    class TestResponder implements ResponseListener<ExecutionEngine.QueryResponse> {
      @Getter Cursor cursor = Cursor.None;
      @Getter List<ExprValue> results = List.of();

      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        cursor = response.getCursor();
        results = response.getResults();
      }

      @Override
      public void onFailure(Exception e) {
        fail(e.getMessage());
      }
    }

    Request request1 = TestUtils.seedDocRequest("test", "1");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = TestUtils.seedDocRequest("test", "2");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);
    Request request3 = TestUtils.seedDocRequest("test", "3");
    request3.setJsonEntity("{\"name\": \"again\", \"age\": 40}");
    client().performRequest(request3);

    OpenSearchIndex index = new OpenSearchIndex(client, defaultSettings(), "test");
    LogicalPlan initialPlan =
        new LogicalPaginate(
            1,
            List.of(
                new LogicalProject(
                    new LogicalRelation("test", index),
                    List.of(
                        DSL.named("name", DSL.ref("name", ExprCoreType.STRING)),
                        DSL.named("age", DSL.ref("age", ExprCoreType.LONG))),
                    List.of())));
    List<TestResponder> pages = new ArrayList<>();
    TestResponder firstPage = new TestResponder();
    queryService.executePlan(initialPlan, PlanContext.emptyPlanContext(), firstPage);
    pages.add(firstPage);
    assertNotNull(planSerializer.convertToPlan(firstPage.getCursor().toString()));

    for (int continuation = 0; continuation < 3; continuation++) {
      Cursor cursor = pages.get(pages.size() - 1).getCursor();
      assertFalse("data page must provide a continuation cursor", cursor.equals(Cursor.None));
      TestResponder nextPage = new TestResponder();
      queryService.execute(new FetchCursor(cursor.toString()), SQL, nextPage);
      pages.add(nextPage);
    }

    List<ExprValue> rows = new ArrayList<>();
    for (int dataPage = 0; dataPage < 3; dataPage++) {
      assertEquals(1, pages.get(dataPage).getResults().size());
      rows.addAll(pages.get(dataPage).getResults());
    }
    assertEquals(3, rows.size());
    assertEquals("each document must appear exactly once", 3, new HashSet<>(rows).size());
    assertTrue(pages.get(3).getResults().isEmpty());
    assertEquals(Cursor.None, pages.get(3).getCursor());
  }

  @Test
  @SneakyThrows
  public void test_explain_not_supported() {
    var request = new Request("POST", "_plugins/_sql/_explain");
    // Request should be rejected before index names are resolved
    request.setJsonEntity("{ \"query\": \"select * from something\", \"fetch_size\": 10 }");
    var exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
    var response =
        new JSONObject(new String(exception.getResponse().getEntity().getContent().readAllBytes()));
    assertEquals(
        "`explain` feature for paginated requests is not implemented yet.",
        response.getJSONObject("error").getString("details"));

    // Request should be rejected before cursor parsed
    request.setJsonEntity("{ \"cursor\" : \"n:0000\" }");
    exception = assertThrows(ResponseException.class, () -> client().performRequest(request));
    response =
        new JSONObject(new String(exception.getResponse().getEntity().getContent().readAllBytes()));
    assertEquals(
        "Explain of a paged query continuation is not supported. Use `explain` for the initial"
            + " query request.",
        response.getJSONObject("error").getString("details"));
  }

  private Settings defaultSettings() {
    return new Settings() {
      private final Map<Key, Object> defaultSettings =
          new ImmutableMap.Builder<Key, Object>()
              .put(Key.QUERY_SIZE_LIMIT, 200)
              .put(Key.QUERY_BUCKET_SIZE, 1000)
              .put(Key.SQL_CURSOR_KEEP_ALIVE, TimeValue.timeValueMinutes(1))
              .put(Key.FIELD_TYPE_TOLERANCE, true)
              .put(Key.DESERIALIZATION_MAX_DEPTH, DeserializationFilterUtil.DEFAULT_MAX_DEPTH)
              .put(Key.DESERIALIZATION_MAX_REFS, DeserializationFilterUtil.DEFAULT_MAX_REFS)
              .put(Key.DESERIALIZATION_MAX_BYTES, DeserializationFilterUtil.DEFAULT_MAX_BYTES)
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
}

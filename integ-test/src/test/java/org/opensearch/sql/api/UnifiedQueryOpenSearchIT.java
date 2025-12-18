/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration test demonstrating UnifiedQueryCompiler API usage with real OpenSearch cluster.
 *
 * <p>This test shows how external tools (Spark, CLI, etc.) can use the Unified Query API:
 *
 * <ol>
 *   <li>Create a Calcite schema with OpenSearchIndex tables
 *   <li>Build UnifiedQueryContext with the schema
 *   <li>Use UnifiedQueryPlanner to parse PPL query into RelNode
 *   <li>Use UnifiedQueryCompiler to compile RelNode into PreparedStatement
 *   <li>Execute and verify results against real OpenSearch data
 * </ol>
 */
public class UnifiedQueryOpenSearchIT extends PPLIntegTestCase {

  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;
  private UnifiedQueryCompiler compiler;

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    setupUnifiedQueryAPI();
  }

  /**
   * Sets up UnifiedQueryContext with a simple schema that directly uses OpenSearchIndex. This
   * demonstrates how external integrations should configure the API without depending on
   * DataSourceService.
   */
  private void setupUnifiedQueryAPI() throws IOException {
    // Get OpenSearch client from the test infrastructure
    RestHighLevelClient restClient = new InternalRestHighLevelClient(client());
    OpenSearchClient openSearchClient = new OpenSearchRestClient(restClient);
    Settings settings = defaultSettings();

    // Create a simple schema with OpenSearchIndex tables
    AbstractSchema testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                TEST_INDEX_ACCOUNT,
                new OpenSearchIndex(openSearchClient, settings, TEST_INDEX_ACCOUNT));
          }
        };

    // Build Calcite root schema
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    String catalogName = "opensearch";
    rootSchema.add(catalogName, testSchema);

    // Create unified query context
    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(catalogName, testSchema)
            .defaultNamespace(catalogName)
            .build();

    // Initialize planner and compiler
    planner = new UnifiedQueryPlanner(context);
    compiler = new UnifiedQueryCompiler(context);
  }

  @Test
  public void testSimplePPLQueryWithUnifiedAPI() throws Exception {
    // Parse PPL query to logical plan
    String pplQuery =
        String.format(
            "source = opensearch.%s | fields firstname, age | where age > 30 | head 3",
            TEST_INDEX_ACCOUNT);

    RelNode logicalPlan = planner.plan(pplQuery);

    // Compile logical plan to executable statement
    try (PreparedStatement statement = compiler.compile(logicalPlan)) {
      // Execute query against real OpenSearch cluster
      ResultSet resultSet = statement.executeQuery();

      // Verify schema - check column names
      assertEquals("Should have 2 columns", 2, resultSet.getMetaData().getColumnCount());
      assertEquals("firstname", resultSet.getMetaData().getColumnName(1));
      assertEquals("age", resultSet.getMetaData().getColumnName(2));

      // Verify we got results (at least one row)
      assertTrue("Expected at least one result row", resultSet.next());

      // Verify data - age should be > 30
      int age = resultSet.getInt("age");
      assertTrue("Age should be greater than 30, got: " + age, age > 30);

      // Verify firstname is not null
      String firstname = resultSet.getString("firstname");
      assertNotNull("Firstname should not be null", firstname);
      assertFalse("Firstname should not be empty", firstname.isEmpty());
    }
  }

  private Settings defaultSettings() {
    return new Settings() {
      private final Map<Key, Object> defaultSettings =
          new ImmutableMap.Builder<Key, Object>()
              .put(Key.QUERY_SIZE_LIMIT, 200)
              .put(Key.QUERY_BUCKET_SIZE, 1000)
              .put(Key.SEARCH_MAX_BUCKETS, 65535)
              .put(Key.SQL_CURSOR_KEEP_ALIVE, TimeValue.timeValueMinutes(1))
              .put(Key.FIELD_TYPE_TOLERANCE, true)
              .put(Key.CALCITE_ENGINE_ENABLED, true)
              .put(Key.CALCITE_PUSHDOWN_ENABLED, true)
              .put(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR, 0.9)
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

  /** Internal RestHighLevelClient only for testing purpose. */
  static class InternalRestHighLevelClient extends RestHighLevelClient {
    public InternalRestHighLevelClient(RestClient restClient) {
      super(restClient, RestClient::close, Collections.emptyList());
    }
  }
}

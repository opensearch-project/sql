/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.After;
import org.junit.Test;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.InternalRestHighLevelClient;

/**
 * Integration test demonstrating the integration and usage of the Unified Query API with OpenSearch
 * as a data source.
 */
public class UnifiedQueryOpenSearchIT extends PPLIntegTestCase implements ResultSetAssertion {

  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;
  private UnifiedQueryCompiler compiler;

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);

    String catalogName = "opensearch";
    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(catalogName, createOpenSearchSchema())
            .defaultNamespace(catalogName)
            .setting("plugins.query.size_limit", 200)
            .setting("plugins.query.buckets", 1000)
            .setting("search.max_buckets", 65535)
            .setting("plugins.sql.cursor.keep_alive", TimeValue.timeValueMinutes(1))
            .setting("plugins.query.field_type_tolerance", true)
            .setting("plugins.calcite.enabled", true)
            .setting("plugins.calcite.pushdown.enabled", true)
            .setting("plugins.calcite.pushdown.rowcount.estimation.factor", 0.9)
            .build();
    planner = new UnifiedQueryPlanner(context);
    compiler = new UnifiedQueryCompiler(context);
  }

  @After
  public void cleanUp() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void testSimplePPLQueryExecution() throws Exception {
    String pplQuery =
        String.format(
            "source = opensearch.%s | fields firstname, age | where age > 30 | head 3",
            TEST_INDEX_ACCOUNT);

    RelNode logicalPlan = planner.plan(pplQuery);
    try (PreparedStatement statement = compiler.compile(logicalPlan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(col("firstname", VARCHAR), col("age", BIGINT))
          .expectData(row("Amber", 32L), row("Hattie", 36L), row("Dale", 33L));
    }
  }

  @Test
  public void testMultiplePPLQueryExecutionWithSameContext() throws Exception {
    String[] queries = {
      "source = opensearch.%s | fields firstname, age | where age > 30 | head 2",
      "source = opensearch.%s | fields lastname, age | where age < 30 | head 3",
      "source = opensearch.%s | fields state, age | where age > 25 | head 5"
    };

    for (String query : queries) {
      RelNode plan = planner.plan(String.format(query, TEST_INDEX_ACCOUNT));

      try (PreparedStatement stmt = compiler.compile(plan)) {
        ResultSet rs = stmt.executeQuery();
        assertNotNull(rs);
        assertTrue("Expected at least one row for query: " + query, rs.next());
      }
    }
  }

  /**
   * Creates a dynamic schema that creates OpenSearchIndex on-demand for any table name. This allows
   * querying any index without pre-registering it.
   */
  private AbstractSchema createOpenSearchSchema() {
    return new AbstractSchema() {
      private final OpenSearchClient osClient =
          new OpenSearchRestClient(new InternalRestHighLevelClient(client()));

      @Override
      protected Map<String, Table> getTableMap() {
        return new HashMap<>() {
          @Override
          public Table get(Object key) {
            if (!super.containsKey(key)) {
              String indexName = (String) key;
              super.put(indexName, new OpenSearchIndex(osClient, context.getSettings(), indexName));
            }
            return super.get(key);
          }
        };
      }
    };
  }
}

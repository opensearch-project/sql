/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Before;
import org.opensearch.sql.executor.QueryType;

/** Base class for unified query tests providing common test schema and utilities. */
public abstract class UnifiedQueryTestBase {

  /** Default catalog name */
  protected static final String DEFAULT_CATALOG = "catalog";

  /** Test schema containing sample tables for testing */
  protected AbstractSchema testSchema;

  /** Unified query context configured with test schema */
  protected UnifiedQueryContext context;

  /** Unified query planner configured with test context */
  protected UnifiedQueryPlanner planner;

  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("employees", createEmployeesTable());
          }
        };

    context = contextBuilder().build();
    planner = new UnifiedQueryPlanner(context);
  }

  /**
   * Returns the query type for this test class. Subclasses override to test different languages.
   */
  protected QueryType queryType() {
    return QueryType.PPL;
  }

  /**
   * Creates a pre-configured context builder with test schema. Subclasses can override to customize
   * context configuration (e.g., enable profiling).
   */
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder().language(queryType()).catalog(DEFAULT_CATALOG, testSchema);
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  /** Creates employees table with sample data for testing */
  protected Table createEmployeesTable() {
    return SimpleTable.builder()
        .col("id", INTEGER)
        .col("name", VARCHAR)
        .col("age", INTEGER)
        .col("department", VARCHAR)
        .row(new Object[] {1, "Alice", 25, "Engineering"})
        .row(new Object[] {2, "Bob", 35, "Sales"})
        .row(new Object[] {3, "Charlie", 45, "Engineering"})
        .row(new Object[] {4, "Diana", 28, "Marketing"})
        .build();
  }

  /** Reusable scannable table with builder pattern for easy table creation */
  @Builder
  protected static class SimpleTable implements ScannableTable {
    @Singular("col")
    private final Map<String, SqlTypeName> schema;

    @Singular private final List<Object[]> rows;

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      schema.forEach(builder::add);
      return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        SqlNode parent,
        org.apache.calcite.config.CalciteConnectionConfig config) {
      return false;
    }
  }

  /** Fluent helper for asserting query plan results. */
  protected QueryAssert givenQuery(String query) {
    return new QueryAssert(planner.plan(query));
  }

  /** Fluent assertion on a query's logical plan. */
  protected static class QueryAssert {
    private final RelNode plan;

    QueryAssert(RelNode plan) {
      this.plan = plan;
    }

    /** Assert the logical plan matches the expected tree string. */
    public QueryAssert assertPlan(String expected) {
      assertEquals(
          expected.stripTrailing(),
          RelOptUtil.toString(plan).replaceAll("\\r\\n", "\n").stripTrailing());
      return this;
    }

    /** Assert the output field names match. */
    public QueryAssert assertFields(String... names) {
      assertEquals(List.of(names), plan.getRowType().getFieldNames());
      return this;
    }

    /** Access the underlying plan for custom assertions. */
    public RelNode plan() {
      return plan;
    }
  }
}

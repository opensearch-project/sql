/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
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
import org.junit.Before;
import org.opensearch.sql.executor.QueryType;

/** Base class for unified query tests providing common test schema and utilities. */
public abstract class UnifiedQueryTestBase {

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

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  /** Creates employees table with sample data for testing */
  protected Table createEmployeesTable() {
    Map<String, SqlTypeName> schema = new java.util.LinkedHashMap<>();
    schema.put("id", SqlTypeName.INTEGER);
    schema.put("name", SqlTypeName.VARCHAR);
    schema.put("age", SqlTypeName.INTEGER);
    schema.put("department", SqlTypeName.VARCHAR);

    return SimpleTable.builder()
        .schema(schema)
        .row(new Object[] {1, "Alice", 25, "Engineering"})
        .row(new Object[] {2, "Bob", 35, "Sales"})
        .row(new Object[] {3, "Charlie", 45, "Engineering"})
        .row(new Object[] {4, "Diana", 28, "Marketing"})
        .build();
  }

  /** Reusable scannable table with builder pattern for easy table creation */
  @Builder
  protected static class SimpleTable implements ScannableTable {
    private final Map<String, SqlTypeName> schema;

    @Singular private final List<Object[]> rows;

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      List<RelDataType> types = new ArrayList<>();
      List<String> names = new ArrayList<>();
      for (Map.Entry<String, SqlTypeName> entry : schema.entrySet()) {
        names.add(entry.getKey());
        types.add(typeFactory.createSqlType(entry.getValue()));
      }
      return typeFactory.createStructType(types, names);
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
}

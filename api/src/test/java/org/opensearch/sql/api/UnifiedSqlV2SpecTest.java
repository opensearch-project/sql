/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for V2 ANTLR parser features that extend Calcite ANSI SQL. Focuses on differences
 * documented in the SQL compatibility spec's "Differences from Standard SQL" section.
 */
public class UnifiedSqlV2SpecTest extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "employees", createEmployeesTable(),
                "logs-2024-01-01", createEmployeesTable());
          }
        };
    return UnifiedQueryContext.builder().language(queryType()).catalog("catalog", schema);
  }

  @Test
  public void backtickQuotedTableName() {
    // SQL V2 extends Calcite ANSI SQL by supporting MySQL-style backtick-quoted identifiers
    givenQuery("SELECT * FROM catalog.`employees`")
        .assertPlan(
            """
            LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void hyphenatedIndexName() {
    // SQL V2 extends Calcite ANSI SQL by supporting hyphenated index names via backtick quoting
    givenQuery("SELECT * FROM catalog.`logs-2024-01-01`")
        .assertPlan(
            """
            LogicalTableScan(table=[[catalog, logs-2024-01-01]])
            """);
  }

  @Test
  public void matchFunctionNotReservedWord() {
    // SQL V2 extends Calcite ANSI SQL by de-reserving MATCH for use as a function name
    givenQuery("SELECT * FROM catalog.employees WHERE match(name, 'Hattie')")
        .assertPlan(
            """
            LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'Hattie':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void stringToNumberCoercion() {
    // SQL V2 extends Calcite ANSI SQL by lenient string-to-number coercion (MySQL-compatible)
    givenQuery("SELECT * FROM catalog.employees WHERE age > '30'")
        .assertPlan(
            """
            LogicalFilter(condition=[>(SAFE_CAST($2), 30.0E0)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void backslashEscapesInStrings() {
    // SQL V2 extends Calcite ANSI SQL by supporting backslash escapes in single-quoted strings
    givenQuery("SELECT * FROM catalog.employees WHERE name = 'it\\'s me'")
        .assertPlan(
            """
            LogicalFilter(condition=[=($1, 'it''s me')])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void doubleQuotedStringLiteral() {
    // SQL V2 extends Calcite ANSI SQL by treating double-quoted values as string literals
    givenQuery("SELECT * FROM catalog.employees WHERE name = \"hello\"")
        .assertPlan(
            """
            LogicalFilter(condition=[=($1, 'hello')])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}

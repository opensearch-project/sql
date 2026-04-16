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

public class UnifiedSqlSpecTest extends UnifiedQueryTestBase {

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
                "logs-2024-01", createEmployeesTable());
          }
        };
    return UnifiedQueryContext.builder()
        .language(queryType())
        .catalog(DEFAULT_CATALOG, schema)
        .defaultNamespace(DEFAULT_CATALOG);
  }

  @Test
  public void hyphenatedTableIdentifier() {
    givenQuery("SELECT * FROM logs-2024-01")
        .assertPlanContains("LogicalTableScan(table=[[catalog, logs-2024-01]])");
  }

  @Test
  public void backtickQuotedIdentifiers() {
    givenQuery("SELECT `name` FROM employees").assertPlanContains("LogicalProject(name=[$1])");
  }

  @Test
  public void doubleQuotedStringLiteral() {
    givenQuery("SELECT \"Hello\" FROM employees").assertPlanContains("LogicalProject");
  }

  @Test
  public void matchNotReserved() {
    givenQuery("SELECT * FROM employees WHERE match(name, 'Hattie')")
        .assertPlanContains("LogicalFilter");
  }

  @Test
  public void reservedWordAsAlias() {
    givenQuery("SELECT age AS year FROM employees").assertPlanContains("LogicalProject(year=[$2])");
  }

  @Test
  public void limitSyntax() {
    givenQuery("SELECT * FROM employees LIMIT 10").assertPlanContains("LogicalSort(fetch=[10])");
  }

  @Test
  public void selectWithoutFrom() {
    givenQuery("SELECT 1").assertPlanContains("LogicalValues(tuples=[[{ 1 }]])");
  }

  @Test
  public void groupByAlias() {
    givenQuery("SELECT department AS dept, COUNT(*) AS cnt FROM employees GROUP BY dept")
        .assertPlanContains("LogicalAggregate(group=[{0}]");
  }

  @Test
  public void groupByOrdinal() {
    givenQuery("SELECT name, COUNT(*) FROM employees GROUP BY 1")
        .assertPlanContains("LogicalAggregate");
  }

  @Test
  public void castBooleanToInteger() {
    givenQuery("SELECT CAST(true AS INTEGER) FROM employees").assertPlanContains("LogicalProject");
  }

  @Test
  public void integerComparedToString() {
    givenQuery("SELECT * FROM employees WHERE age > '30'").assertPlanContains("LogicalFilter");
  }

  @Test
  public void matchFunction() {
    givenQuery("SELECT * FROM employees WHERE match(name, 'John')")
        .assertPlanContains("match(MAP('field', $1), MAP('query', 'John'))");
  }

  @Test
  public void matchPhraseFunction() {
    givenQuery("SELECT * FROM employees WHERE match_phrase(name, 'quick fox')")
        .assertPlanContains("match_phrase(MAP('field', $1), MAP('query', 'quick fox'))");
  }

  @Test
  public void namedParametersSyntax() {
    givenQuery("SELECT * FROM employees WHERE match_phrase(name, 'quick fox', slop=2)")
        .assertPlanContains("match_phrase(MAP('field', $1), MAP('query', 'quick fox')");
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for relevance search functions in SQL planning path. Functions are registered globally on
 * the root schema via {@link org.opensearch.sql.api.spec.UnifiedFunctionSpec#registerAll}, resolved
 * through Calcite's standard CalciteCatalogReader → toOp() pipeline.
 */
public class UnifiedRelevanceSearchSqlTest extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Test
  public void testMatch() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE "match"(MAP['field', name], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchPhrase() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match_phrase(MAP['field', name], MAP['query', 'John Doe'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[match_phrase(MAP('field', $1), MAP('query', 'John Doe'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchBoolPrefix() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match_bool_prefix(MAP['field', name], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[match_bool_prefix(MAP('field', $1), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchPhrasePrefix() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match_phrase_prefix(MAP['field', name], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[match_phrase_prefix(MAP('field', $1), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMultiMatch() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE multi_match(\
            MAP['fields', MAP['name', 1.0, 'department', 2.0]], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[multi_match(MAP('fields', MAP(CAST('name'):CHAR(10) NOT NULL, 1.0:DECIMAL(2, 1), 'department', 2.0:DECIMAL(2, 1))), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSimpleQueryString() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE simple_query_string(\
            MAP['fields', MAP['name', 1.0]], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[simple_query_string(MAP('fields', MAP('name', 1.0:DECIMAL(2, 1))), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testQueryString() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE query_string(\
            MAP['fields', MAP['name', 1.0]], MAP['query', 'John'])\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalFilter(condition=[query_string(MAP('fields', MAP('name', 1.0:DECIMAL(2, 1))), MAP('query', 'John'))])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}

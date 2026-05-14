/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for relevance search functions through the V2 ANTLR parser path. Covers match,
 * match_phrase, multi_match, match_bool_prefix, match_phrase_prefix, simple_query_string, and
 * query_string with bracket syntax.
 */
public class UnifiedRelevanceSearchSqlV2Test extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Test
  public void match() {
    givenQuery("SELECT * FROM catalog.employees WHERE match(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void matchWithOptions() {
    givenQuery(
            "SELECT * FROM catalog.employees WHERE match(name, 'John', operator='AND', boost=2.0)")
        .assertPlan(
            """
            LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'John':VARCHAR), MAP('operator', 'AND':VARCHAR), MAP('boost', '2.0':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void matchPhrase() {
    givenQuery("SELECT * FROM catalog.employees WHERE match_phrase(name, 'John Doe')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_phrase(MAP('field', $1), MAP('query', 'John Doe':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void matchBoolPrefix() {
    givenQuery("SELECT * FROM catalog.employees WHERE match_bool_prefix(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_bool_prefix(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void matchPhrasePrefix() {
    givenQuery("SELECT * FROM catalog.employees WHERE match_phrase_prefix(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_phrase_prefix(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void multiMatchBracketSyntax() {
    givenQuery("SELECT * FROM catalog.employees WHERE multi_match(['name', 'department'], 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[multi_match(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE, 'department':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void multiMatchWithFieldBoost() {
    givenQuery(
            """
            SELECT * FROM catalog.employees\
             WHERE multi_match(['name' ^ 2.0, 'department'], 'John')\
            """)
        .assertPlan(
            """
            LogicalFilter(condition=[multi_match(MAP('fields', MAP('name':VARCHAR, 2.0E0:DOUBLE, 'department':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void simpleQueryStringBracketSyntax() {
    givenQuery(
            """
            SELECT * FROM catalog.employees\
             WHERE simple_query_string(['name', 'department'], 'John')\
            """)
        .assertPlan(
            """
            LogicalFilter(condition=[simple_query_string(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE, 'department':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void queryStringBracketSyntax() {
    givenQuery(
            """
            SELECT * FROM catalog.employees\
             WHERE query_string(['name', 'department'], 'John')\
            """)
        .assertPlan(
            """
            LogicalFilter(condition=[query_string(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE, 'department':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void matchCombinedWithBooleanFilter() {
    givenQuery("SELECT * FROM catalog.employees WHERE match(name, 'John') AND age > 25")
        .assertPlan(
            """
            LogicalFilter(condition=[AND(match(MAP('field', $1), MAP('query', 'John':VARCHAR)), >($2, 25))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}

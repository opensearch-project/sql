/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for relevance search functions in SQL planning path using V2/PPL syntax. Mirrors the PPL
 * tests in {@link UnifiedRelevanceSearchTest} with equivalent SQL queries. Both paths produce
 * identical MAP-based plans for pushdown rules.
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
            WHERE match(name, 'John')\
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
            WHERE match_phrase(name, 'John Doe')\
            """)
        .assertPlanContains("match_phrase(MAP('field', $1), MAP('query', 'John Doe'))");
  }

  @Test
  public void testMatchBoolPrefix() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match_bool_prefix(name, 'John')\
            """)
        .assertPlanContains("match_bool_prefix(MAP('field', $1), MAP('query', 'John'))");
  }

  @Test
  public void testMatchPhrasePrefix() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match_phrase_prefix(name, 'John')\
            """)
        .assertPlanContains("match_phrase_prefix(MAP('field', $1), MAP('query', 'John'))");
  }

  @Test
  public void testMultiMatch() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE multi_match(name, 'John')\
            """)
        .assertPlanContains("multi_match(MAP('fields', $1), MAP('query', 'John'))");
  }

  @Test
  public void testSimpleQueryString() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE simple_query_string(name, 'John')\
            """)
        .assertPlanContains("simple_query_string(MAP('fields', $1), MAP('query', 'John'))");
  }

  @Test
  public void testQueryString() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE query_string(name, 'John')\
            """)
        .assertPlanContains("query_string(MAP('fields', $1), MAP('query', 'John'))");
  }

  @Test
  public void testMatchWithOptions() {
    givenQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match(name, 'John', operator='AND', boost=2.0)\
            """)
        .assertPlanContains(
            "match(MAP('field', $1), MAP('query', 'John'),"
                + " MAP('operator', 'AND'), MAP('boost', 2.0:DECIMAL(2, 1)))");
  }

  @Test
  public void testMatchMissingArguments() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE match('John')\
            """)
        .assertErrorMessage(
            "No match found for function signature match(<(CHAR(5), CHAR(4)) MAP>)");
  }

  @Test
  public void testUnknownRelevanceFunction() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE unknown_relevance(name, 'John')\
            """)
        .assertErrorMessage(
            "No match found for function signature unknown_relevance(<CHARACTER>, <CHARACTER>)");
  }

  @Test
  public void testNonRelevanceFunctionUnaffectedByRewriter() {
    givenQuery(
            """
            SELECT upper(name) FROM catalog.employees\
            """)
        .assertPlan(
            """
            LogicalProject(EXPR$0=[UPPER($1)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  // FIXME: Calcite's SQL parser does not support V2 bracket field list syntax ['field1', 'field2'].
  //  Multi-field relevance functions only accept a single column reference in the Calcite SQL path.

  @Test
  public void testMultiMatchBracketSyntaxNotSupported() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE multi_match(['name', 'department'], 'John')\
            """)
        .assertErrorMessage("Encountered \"[\" at line");
  }

  @Test
  public void testMultiMatchFieldBoostNotSupported() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE multi_match(['name' ^ 2.0, 'department'], 'John')\
            """)
        .assertErrorMessage("Encountered \"[\" at line");
  }

  @Test
  public void testSimpleQueryStringBracketSyntaxNotSupported() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE simple_query_string(['name', 'department'], 'John')\
            """)
        .assertErrorMessage("Encountered \"[\" at line");
  }

  @Test
  public void testQueryStringBracketSyntaxNotSupported() {
    givenInvalidQuery(
            """
            SELECT * FROM catalog.employees
            WHERE query_string(['name', 'department'], 'John')\
            """)
        .assertErrorMessage("Encountered \"[\" at line");
  }
}

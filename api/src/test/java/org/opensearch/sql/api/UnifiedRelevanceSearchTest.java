/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.junit.Test;

/** Tests for relevance search functions in PPL planning path. */
public class UnifiedRelevanceSearchTest extends UnifiedQueryTestBase {

  @Test
  public void testMatch() {
    givenQuery("source=catalog.employees | where match(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchPhrase() {
    givenQuery("source=catalog.employees | where match_phrase(name, 'John Doe')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_phrase(MAP('field', $1), MAP('query', 'John Doe':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchBoolPrefix() {
    givenQuery("source=catalog.employees | where match_bool_prefix(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_bool_prefix(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMatchPhrasePrefix() {
    givenQuery("source=catalog.employees | where match_phrase_prefix(name, 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[match_phrase_prefix(MAP('field', $1), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMultiMatch() {
    givenQuery("source=catalog.employees | where multi_match(['name', 'department'], 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[multi_match(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE, 'department':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSimpleQueryString() {
    givenQuery("source=catalog.employees | where simple_query_string(['name'], 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[simple_query_string(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testQueryString() {
    givenQuery("source=catalog.employees | where query_string(['name'], 'John')")
        .assertPlan(
            """
            LogicalFilter(condition=[query_string(MAP('fields', MAP('name':VARCHAR, 1.0E0:DOUBLE)), MAP('query', 'John':VARCHAR))])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}

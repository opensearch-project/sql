/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.filteredAggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.tree.Sort.NullOrder;
import static org.opensearch.sql.ast.tree.Sort.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QuerySpecificationContext;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class QuerySpecificationTest {

  @Test
  void can_collect_group_by_items_in_group_by_clause() {
    QuerySpecification querySpec =
        collect("SELECT name, ABS(age) FROM test GROUP BY name, ABS(age)");

    assertEquals(
        ImmutableList.of(qualifiedName("name"), function("ABS", qualifiedName("age"))),
        querySpec.getGroupByItems());
  }

  @Test
  void can_collect_select_items_in_select_clause() {
    QuerySpecification querySpec = collect("SELECT name, ABS(age) FROM test");

    assertEquals(
        ImmutableList.of(qualifiedName("name"), function("ABS", qualifiedName("age"))),
        querySpec.getSelectItems());
  }

  @Test
  void can_collect_aggregators_in_select_clause() {
    QuerySpecification querySpec =
        collect("SELECT name, AVG(age), SUM(balance) FROM test GROUP BY name");

    assertEquals(
        ImmutableSet.of(
            alias("AVG(age)", aggregate("AVG", qualifiedName("age"))),
            alias("SUM(balance)", aggregate("SUM", qualifiedName("balance")))),
        querySpec.getAggregators());
  }

  @Test
  void can_collect_nested_aggregators_in_select_clause() {
    QuerySpecification querySpec =
        collect("SELECT name, ABS(1 + AVG(age)) FROM test GROUP BY name");

    assertEquals(
        ImmutableSet.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        querySpec.getAggregators());
  }

  @Test
  void can_collect_alias_in_select_clause() {
    QuerySpecification querySpec = collect("SELECT name AS n FROM test GROUP BY n");

    assertEquals(ImmutableMap.of("n", qualifiedName("name")), querySpec.getSelectItemsByAlias());
  }

  @Test
  void should_deduplicate_same_aggregators() {
    QuerySpecification querySpec =
        collect("SELECT AVG(age), AVG(balance), AVG(age) FROM test GROUP BY name");

    assertEquals(
        ImmutableSet.of(
            alias("AVG(age)", aggregate("AVG", qualifiedName("age"))),
            alias("AVG(balance)", aggregate("AVG", qualifiedName("balance")))),
        querySpec.getAggregators());
  }

  @Test
  void can_collect_sort_options_in_order_by_clause() {
    assertEquals(
        ImmutableList.of(new SortOption(null, null)),
        collect("SELECT name FROM test ORDER BY name").getOrderByOptions());

    assertEquals(
        ImmutableList.of(new SortOption(SortOrder.ASC, NullOrder.NULL_LAST)),
        collect("SELECT name FROM test ORDER BY name ASC NULLS LAST").getOrderByOptions());

    assertEquals(
        ImmutableList.of(new SortOption(SortOrder.DESC, NullOrder.NULL_FIRST)),
        collect("SELECT name FROM test ORDER BY name DESC NULLS FIRST").getOrderByOptions());
  }

  @Test
  void should_skip_sort_items_in_window_function() {
    assertEquals(
        1,
        collect("SELECT name, RANK() OVER(ORDER BY age) " + "FROM test ORDER BY name")
            .getOrderByOptions()
            .size());
  }

  @Test
  void can_collect_filtered_aggregation() {
    assertEquals(
        ImmutableSet.of(
            alias(
                "AVG(age) FILTER(WHERE age > 20)",
                filteredAggregate(
                    "AVG",
                    qualifiedName("age"),
                    function(">", qualifiedName("age"), intLiteral(20))))),
        collect("SELECT AVG(age) FILTER(WHERE age > 20) FROM test").getAggregators());
  }

  private QuerySpecification collect(String query) {
    QuerySpecification querySpec = new QuerySpecification();
    querySpec.collect(parse(query), query);
    return querySpec;
  }

  private QuerySpecificationContext parse(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.querySpecification();
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryParserTest extends UnifiedQueryTestBase {

  @Test
  public void testParseSource() {
    assertEqual(
        "source = catalog.employees",
        project(relation(qualifiedName("catalog", "employees")), AllFieldsExcludeMeta.of()));
  }

  @Test
  public void testParseFilter() {
    assertEqual(
        "source = catalog.employees | where age > 30",
        project(
            filter(
                relation(qualifiedName("catalog", "employees")),
                compare(">", field("age"), intLiteral(30))),
            AllFieldsExcludeMeta.of()));
  }

  @Test
  public void testParseEval() {
    assertEqual(
        "source = catalog.employees | eval f = abs(id)",
        project(
            eval(
                relation(qualifiedName("catalog", "employees")),
                let(field("f"), function("abs", field("id")))),
            AllFieldsExcludeMeta.of()));
  }

  @Test
  public void testParseStats() {
    assertEqual(
        "source = catalog.employees | stats count(age) by department",
        project(
            agg(
                relation(qualifiedName("catalog", "employees")),
                exprList(alias("count(age)", aggregate("count", field("age")))),
                emptyList(),
                exprList(alias("department", field("department"))),
                defaultStatsArgs()),
            AllFieldsExcludeMeta.of()));
  }

  @Test
  public void testSyntaxErrorThrows() {
    assertThrows(SyntaxCheckException.class, () -> context.getParser().parse("not a valid query"));
  }

  @Test
  public void deeplyNestedExpressionShouldBeRejected() throws Exception {
    String key = Settings.Key.MAX_EXPRESSION_DEPTH.getKeyValue();
    try (UnifiedQueryContext ppl =
            UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog(DEFAULT_CATALOG, testSchema)
                .setting(key, 20)
                .build();
        UnifiedQueryContext sql =
            UnifiedQueryContext.builder()
                .language(QueryType.SQL)
                .catalog(DEFAULT_CATALOG, testSchema)
                .setting(key, 20)
                .build()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> ppl.getParser().parse("source = catalog.employees | where " + orChain(30)));
      assertThrows(
          IllegalArgumentException.class,
          () -> sql.getParser().parse("SELECT * FROM catalog.employees WHERE " + orChain(30)));
    }
  }

  private String orChain(int terms) {
    StringBuilder sb = new StringBuilder("age = 1");
    for (int i = 2; i <= terms; i++) {
      sb.append(" or age = ").append(i);
    }
    return sb.toString();
  }

  private void assertEqual(String query, UnresolvedPlan expected) {
    UnresolvedPlan actual = (UnresolvedPlan) context.getParser().parse(query);
    assertEquals(expected, actual);
  }
}

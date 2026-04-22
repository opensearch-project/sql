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
import static org.opensearch.sql.ast.dsl.AstDSL.allFields;
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
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.error.ErrorReport;

public class UnifiedQueryParserTest extends UnifiedQueryTestBase {

  @Test
  public void testParseSource() {
    assertEqual(
        "source = catalog.employees",
        project(relation(qualifiedName("catalog", "employees")), allFields()));
  }

  @Test
  public void testParseFilter() {
    assertEqual(
        "source = catalog.employees | where age > 30",
        project(
            filter(
                relation(qualifiedName("catalog", "employees")),
                compare(">", field("age"), intLiteral(30))),
            allFields()));
  }

  @Test
  public void testParseEval() {
    assertEqual(
        "source = catalog.employees | eval f = abs(id)",
        project(
            eval(
                relation(qualifiedName("catalog", "employees")),
                let(field("f"), function("abs", field("id")))),
            allFields()));
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
            allFields()));
  }

  @Test
  public void testSyntaxErrorThrows() {
    assertThrows(ErrorReport.class, () -> context.getParser().parse("not a valid query"));
  }

  private void assertEqual(String query, UnresolvedPlan expected) {
    UnresolvedPlan actual = (UnresolvedPlan) context.getParser().parse(query);
    assertEquals(expected, actual);
  }
}

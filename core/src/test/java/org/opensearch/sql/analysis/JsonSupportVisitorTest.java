/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

class JsonSupportVisitorTest {
  @Test
  public void visitLiteralInProject() {
    UnresolvedPlan project = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.intLiteral(1));
    assertThrows(UnsupportedOperationException.class,
        () -> project.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitLiteralOutsideProject() {
    Literal intLiteral = AstDSL.intLiteral(1);
    assertNull(intLiteral.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitCastInProject() {
    UnresolvedPlan project = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.cast(AstDSL.intLiteral(1), AstDSL.stringLiteral("INT")));
    assertThrows(UnsupportedOperationException.class,
        () -> project.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitCastOutsideProject() {
    UnresolvedExpression intCast = AstDSL.cast(
        AstDSL.intLiteral(1),
        AstDSL.stringLiteral("INT"));
    assertNull(intCast.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAliasInProject() {
    UnresolvedPlan project = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.alias("alias", AstDSL.intLiteral(1)));
    assertThrows(UnsupportedOperationException.class,
        () -> project.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAliasInProjectWithUnsupportedDelegated() {
    UnresolvedPlan project = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.alias("alias", AstDSL.intLiteral(1), "alias"));
    assertThrows(UnsupportedOperationException.class,
        () -> project.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAliasInProjectWithSupportedDelegated() {
    UnresolvedPlan project = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.alias("alias", AstDSL.field("field")));
    assertNull(project.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAliasOutsideProject() {
    UnresolvedExpression alias = AstDSL.alias("alias", AstDSL.intLiteral(1));
    assertNull(alias.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitFunctionInProject() {
    UnresolvedPlan function = AstDSL.project(
        AstDSL.relation("table", "table"),
        AstDSL.function("abs", AstDSL.intLiteral(-1)));
    assertThrows(UnsupportedOperationException.class,
        () -> function.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitFunctionOutsideProject() {
    UnresolvedExpression function = AstDSL.function("abs", AstDSL.intLiteral(-1));
    assertNull(function.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAggregationWithGroupExprList() {
    UnresolvedPlan projectAggr = AstDSL.project(AstDSL.agg(
        AstDSL.relation("table", "table"),
        Collections.emptyList(),
        Collections.emptyList(),
        ImmutableList.of(AstDSL.alias("alias", qualifiedName("integer_value"))),
        Collections.emptyList()));
    assertThrows(UnsupportedOperationException.class,
        () -> projectAggr.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }

  @Test
  public void visitAggregationWithAggExprList() {
    UnresolvedPlan aggregation = AstDSL.agg(
        AstDSL.relation("table", "table"),
        ImmutableList.of(
            AstDSL.alias("AVG(alias)",
                AstDSL.aggregate("AVG",
                    qualifiedName("integer_value")))),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
    assertNull(aggregation.accept(new JsonSupportVisitor(), new JsonSupportVisitorContext()));
  }
}

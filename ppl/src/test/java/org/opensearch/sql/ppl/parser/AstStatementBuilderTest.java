/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.executor.QueryType.PPL;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

public class AstStatementBuilderTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Mock private Settings settings;

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Test
  public void buildQueryStatement() {
    assertEqual(
        "search source=t a=1",
        new Query(
            project(filter(relation("t"), compare("=", field("a"), intLiteral(1))), AllFields.of()),
            0,
            PPL));
  }

  @Test
  public void buildExplainStatement() {
    assertExplainEqual(
        "search source=t a=1",
        new Explain(
            new Query(
                project(
                    filter(relation("t"), compare("=", field("a"), intLiteral(1))), AllFields.of()),
                0,
                PPL),
            PPL));
  }

  private void assertEqual(String query, Statement expectedStatement) {
    Node actualPlan = plan(query, false);
    assertEquals(expectedStatement, actualPlan);
  }

  private void assertExplainEqual(String query, Statement expectedStatement) {
    Node actualPlan = plan(query, true);
    assertEquals(expectedStatement, actualPlan);
  }

  private Node plan(String query, boolean isExplain) {
    final AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
    return builder.visit(parser.parse(query));
  }
}

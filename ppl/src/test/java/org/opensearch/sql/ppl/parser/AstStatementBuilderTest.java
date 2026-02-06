/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.*;
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

  private PPLSyntaxParser parser = new PPLSyntaxParser();

  @Test
  public void buildQueryStatement() {
    assertEqual(
        "search source=t | where a=1",
        new Query(
            project(filter(relation("t"), compare("=", field("a"), intLiteral(1))), AllFields.of()),
            0,
            PPL));
    assertEqual(
        "search source=t a=1",
        new Query(project(search(relation("t"), "a:1"), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildExplainStatement() {
    assertExplainEqual(
        "search source=t | where a=1",
        new Explain(
            new Query(
                project(
                    filter(relation("t"), compare("=", field("a"), intLiteral(1))), AllFields.of()),
                0,
                PPL),
            PPL));
    assertExplainEqual(
        "search source=t a=1",
        new Explain(new Query(project(search(relation("t"), "a:1"), AllFields.of()), 0, PPL), PPL));
  }

  @Test
  public void buildQueryStatementWithFetchSize() {
    // When fetchSize > 0, a Head node is injected below Project (addSelectAll wraps the top)
    assertEqualWithFetchSize(
        "search source=t a=1",
        100,
        new Query(project(head(search(relation("t"), "a:1"), 100, 0), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildQueryStatementWithFetchSizeZero() {
    // fetchSize=0 means use system default - no Head node injected
    assertEqualWithFetchSize(
        "search source=t a=1",
        0,
        new Query(project(search(relation("t"), "a:1"), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildQueryStatementWithLargeFetchSize() {
    assertEqualWithFetchSize(
        "search source=t a=1",
        10000,
        new Query(project(head(search(relation("t"), "a:1"), 10000, 0), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildQueryStatementWithFetchSizeAndSmallerHead() {
    // User query has head 3, fetchSize=10
    // Head(10) wraps Head(3), then Project(*) wraps on top
    // The inner head 3 limits first, so only 3 rows are returned
    assertEqualWithFetchSize(
        "source=t | head 3",
        10,
        new Query(project(head(head(relation("t"), 3, 0), 10, 0), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildQueryStatementWithFetchSizeSmallerThanHead() {
    // User query has head 100, fetchSize=5
    // Head(5) wraps Head(100), then Project(*) wraps on top
    // The outer head 5 limits, so only 5 rows are returned
    assertEqualWithFetchSize(
        "source=t | head 100",
        5,
        new Query(project(head(head(relation("t"), 100, 0), 5, 0), AllFields.of()), 0, PPL));
  }

  @Test
  public void buildQueryStatementWithFetchSizeAndHeadWithOffset() {
    // User query has head 3 from 1 (with offset), fetchSize=10
    // The inner head offset is preserved, outer Head always has offset 0
    assertEqualWithFetchSize(
        "source=t | head 3 from 1",
        10,
        new Query(project(head(head(relation("t"), 3, 1), 10, 0), AllFields.of()), 0, PPL));
  }

  private void assertEqualWithFetchSize(String query, int fetchSize, Statement expectedStatement) {
    Node actualPlan = planWithFetchSize(query, fetchSize);
    assertEquals(expectedStatement, actualPlan);
  }

  private Node planWithFetchSize(String query, int fetchSize) {
    final AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder()
                .isExplain(false)
                .fetchSize(fetchSize)
                .build());
    return builder.visit(parser.parse(query));
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
            new AstBuilder(query),
            AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
    return builder.visit(parser.parse(query));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;

import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

/**
 * Integration test to demonstrate percentile shortcut functions working end-to-end.
 * This test shows that perc<percent> and p<percent> functions are correctly
 * transformed to percentile(<value>, <percent>) calls.
 */
public class PercentileShortcutIntegrationTest extends AstBuilderTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, null);
    return astBuilder.visit(parser.parse(query));
  }

  @Test
  public void testPercentileShortcutFunctionsWorkEndToEnd() {
    // Test perc20 shortcut
    assertEqual(
        "source=accounts | stats perc20(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "perc20(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(20.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));

    // Test p50 shortcut
    assertEqual(
        "source=accounts | stats p50(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "p50(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(50.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));

    // Test p95 shortcut
    assertEqual(
        "source=accounts | stats p95(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "p95(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(95.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));

    // Test case insensitive
    assertEqual(
        "source=accounts | stats PERC99(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "PERC99(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(99.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }
}
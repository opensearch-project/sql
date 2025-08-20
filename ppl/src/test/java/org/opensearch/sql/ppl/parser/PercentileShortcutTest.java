/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;

import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

public class PercentileShortcutTest extends AstBuilderTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, null);
    return astBuilder.visit(parser.parse(query));
  }

  @Test
  public void test_perc20_shortcut() {
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
  }

  @Test
  public void test_perc95_shortcut() {
    assertEqual(
        "source=accounts | stats perc95(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "perc95(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(95.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void test_p50_shortcut() {
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
  }

  @Test
  public void test_perc99_5_shortcut() {
    assertEqual(
        "source=accounts | stats perc99.5(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "perc99.5(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(99.5))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }



  @Test
  public void test_perc0_shortcut() {
    assertEqual(
        "source=accounts | stats perc0(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "perc0(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(0.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void test_perc100_shortcut() {
    assertEqual(
        "source=accounts | stats perc100(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "perc100(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(100.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test(expected = SyntaxCheckException.class)
  public void test_invalid_percentile_over_100() {
    plan("source=accounts | stats perc101(age)");
  }

  @Test
  public void test_case_insensitive() {
    assertEqual(
        "source=accounts | stats PERC25(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "PERC25(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(25.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void test_mixed_case() {
    assertEqual(
        "source=accounts | stats P75(age)",
        agg(
            relation("accounts"),
            exprList(
                alias(
                    "P75(age)",
                    aggregate("percentile", field("age"), unresolvedArg("percent", doubleLiteral(75.0))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }
}
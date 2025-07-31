/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;

import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.junit.Test;

public class CalcitePPLTimechartTest {

  @Test
  public void testTimechartBasicSyntax() {
    String ppl = "source=events | timechart count()";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithSpanSyntax() {
    String ppl = "source=events | timechart span(@timestamp, 1m) count()";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  @Test
  public void testTimechartWithBySyntax() {
    String ppl = "source=events | timechart count() by url";
    UnresolvedPlan plan = parsePPL(ppl);
    assertNotNull(plan);
  }

  private UnresolvedPlan parsePPL(String query) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    AstBuilder astBuilder = new AstBuilder(query);
    return astBuilder.visit(parser.parse(query));
  }
}
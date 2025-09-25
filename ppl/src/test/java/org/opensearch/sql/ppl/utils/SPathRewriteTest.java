/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.spath;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class SPathRewriteTest {
  private final Settings settings = Mockito.mock(Settings.class);
  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }

  // Control test to make sure something fundamental hasn't changed about the json_extract parsing
  @Test
  public void testEvalControl() {
    assertEquals(
        eval(
            relation("t"),
            let(field("o"), function("json_extract", field("f"), stringLiteral("simple.nested")))),
        plan("source = t | eval o=json_extract(f, \"simple.nested\")"));
  }

  @Test
  public void testSpathSimpleRewrite() {
    SPath sp = spath(relation("t"), "f", "o", "simple.nested");
    Eval ev = (Eval) plan("source = t | eval o=json_extract(f, \"simple.nested\")");

    assertEquals(ev, sp.rewriteAsEval());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpathMissingInputArgumentHandling() {
    plan("source = t | spath path=a output=a");
  }

  @Test
  public void testSpathArgumentDeshuffle() {
    assertEquals(plan("source = t | spath path=a input=a"), plan("source = t | spath input=a a"));
  }
}

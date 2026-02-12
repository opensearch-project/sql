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
  public void testSpathExtractAllRewrite() {
    // spath input=a output=o (no path) -> eval o=json_extract_all(a)
    SPath sp = (SPath) plan("source = t | spath input=a output=o");
    assertEquals(
        eval(relation("t"), let(field("o"), function("json_extract_all", field("a")))),
        sp.rewriteAsExtractAllEval());
  }

  @Test
  public void testSpathExtractAllDefaultOutput() {
    // spath input=a (no path, no output) -> eval a=json_extract_all(a)
    SPath sp = (SPath) plan("source = t | spath input=a");
    assertEquals(
        eval(relation("t"), let(field("a"), function("json_extract_all", field("a")))),
        sp.rewriteAsExtractAllEval());
  }

  @Test
  public void testSpathArgumentDeshuffle() {
    assertEquals(plan("source = t | spath path=a input=a"), plan("source = t | spath input=a a"));
  }

  @Test
  public void testSpathEscapedParse() {
    SPath sp =
        (SPath) plan("source = t | spath input=f output=o path=\"attributes.['cluster.name']\"");
    Eval ev = (Eval) plan("source = t | eval o=json_extract(f, \"attributes.['cluster.name']\")");

    assertEquals(ev, sp.rewriteAsEval());
  }

  @Test
  public void testSpathEscapedSpaces() {
    SPath sp = (SPath) plan("source = t | spath input=f output=o path=\"['abc def ghi']\"");
    Eval ev = (Eval) plan("source = t | eval o=json_extract(f, \"['abc def ghi']\")");

    assertEquals(ev, sp.rewriteAsEval());
  }
}

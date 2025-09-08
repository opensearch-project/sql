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
import org.opensearch.sql.ppl.AstPlanningTest;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

public class SPathRewriteTest extends AstPlanningTest {
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

  @Test(expected = IllegalArgumentException.class)
  public void testSpathMissingPathArgumentHandling() {
    plan("source = t | spath input=a output=a");
  }
}

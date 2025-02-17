/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import java.util.List;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

@RunWith(Parameterized.class)
public class AstNowLikeFunctionTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Mock private Settings settings;

  /**
   * Set parameterized values used in test.
   *
   * @param name Function name
   * @param hasFsp Whether function has fsp argument
   * @param hasShortcut Whether function has shortcut (call without `()`)
   */
  public AstNowLikeFunctionTest(String name, Boolean hasFsp, Boolean hasShortcut) {
    this.name = name;
    this.hasFsp = hasFsp;
    this.hasShortcut = hasShortcut;
  }

  /**
   * Returns function data to test.
   *
   * @return An iterable.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> functionNames() {
    return List.of(
        new Object[][] {
          {"now", false, false},
          {"current_timestamp", false, false},
          {"localtimestamp", false, false},
          {"localtime", false, false},
          {"sysdate", true, false},
          {"curtime", false, false},
          {"current_time", false, false},
          {"curdate", false, false},
          {"current_date", false, false},
          {"utc_date", false, false},
          {"utc_time", false, false},
          {"utc_timestamp", false, false}
        });
  }

  private final String name;
  private final boolean hasFsp;
  private final boolean hasShortcut;

  @Test
  public void test_function_call_eval() {
    assertEqual(
        eval(relation("t"), let(field("r"), function(name))), "source=t | eval r=" + name + "()");
  }

  @Test
  public void test_shortcut_eval() {
    Assume.assumeTrue(hasShortcut);
    assertEqual(eval(relation("t"), let(field("r"), function(name))), "source=t | eval r=" + name);
  }

  @Test
  public void test_function_call_where() {
    assertEqual(
        filter(relation("t"), compare("=", field("a"), function(name))),
        "search source=t | where a=" + name + "()");
  }

  @Test
  public void test_shortcut_where() {
    Assume.assumeTrue(hasShortcut);
    assertEqual(
        filter(relation("t"), compare("=", field("a"), function(name))),
        "search source=t | where a=" + name);
  }

  @Test
  public void test_function_call_fsp() {
    Assume.assumeTrue(hasFsp);
    assertEqual(
        filter(relation("t"), compare("=", field("a"), function(name, intLiteral(0)))),
        "search source=t | where a=" + name + "(0)");
  }

  protected void assertEqual(Node expectedPlan, String query) {
    Node actualPlan = plan(query);
    assertEquals(expectedPlan, actualPlan);
  }

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(new AstExpressionBuilder(), settings, query);
    return astBuilder.visit(parser.parse(query));
  }
}

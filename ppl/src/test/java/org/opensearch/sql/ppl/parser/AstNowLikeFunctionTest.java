/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.constantFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

@RunWith(Parameterized.class)
public class AstNowLikeFunctionTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  /**
   * Set parameterized values used in test.
   * @param name Function name
   * @param hasFsp Whether function has fsp argument
   * @param hasShortcut Whether function has shortcut (call without `()`)
   * @param isConstantFunction Whether function has constant value
   */
  public AstNowLikeFunctionTest(String name, Boolean hasFsp, Boolean hasShortcut,
                                Boolean isConstantFunction) {
    this.name = name;
    this.hasFsp = hasFsp;
    this.hasShortcut = hasShortcut;
    this.isConstantFunction = isConstantFunction;
  }

  /**
   * Returns function data to test.
   * @return An iterable.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> functionNames() {
    return List.of(new Object[][]{
        {"now", false, false, true},
        {"current_timestamp", false, false, true},
        {"localtimestamp", false, false, true},
        {"localtime", false, false, true},
        {"sysdate", true, false, false},
        {"curtime", false, false, true},
        {"current_time", false, false, true},
        {"curdate", false, false, true},
        {"current_date", false, false, true},
        {"utc_date", false, true, true},
        {"utc_time", false, true, true},
        {"utc_timestamp", false, true, true}
    });
  }

  private final String name;
  private final Boolean hasFsp;
  private final Boolean hasShortcut;
  private final Boolean isConstantFunction;

  @Test
  public void test_now_like_functions() {
    for (var call : hasShortcut ? List.of(name, name + "()") : List.of(name + "()")) {
      assertEqual("source=t | eval r=" + call,
          eval(
              relation("t"),
              let(
                  field("r"),
                  (isConstantFunction ? constantFunction(name) : function(name))
              )
          ));

      assertEqual("search source=t | where a=" + call,
          filter(
              relation("t"),
              compare("=", field("a"),
                  (isConstantFunction ? constantFunction(name) : function(name)))
          )
      );
    }
    // Unfortunately, only real functions (not ConstantFunction) might have `fsp` now.
    if (hasFsp) {
      assertEqual("search source=t | where a=" + name + "(0)",
          filter(
              relation("t"),
              compare("=", field("a"), function(name, intLiteral(0)))
          )
      );
    }
  }

  protected void assertEqual(String query, Node expectedPlan) {
    Node actualPlan = plan(query);
    assertEquals(expectedPlan, actualPlan);
  }

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(new AstExpressionBuilder(), query);
    return astBuilder.visit(parser.parse(query));
  }
}

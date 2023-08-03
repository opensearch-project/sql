/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertNotEquals;

import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class NowLikeFunctionParserTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  /**
   * Set parameterized values used in test.
   * @param name Function name
   * @param hasFsp Whether function has fsp argument
   * @param hasShortcut Whether function has shortcut (call without `()`)
   */
  public NowLikeFunctionParserTest(String name, Boolean hasFsp, Boolean hasShortcut) {
    this.name = name;
    this.hasFsp = hasFsp;
    this.hasShortcut = hasShortcut;
  }

  /**
   * Returns function data to test.
   * @return An iterable.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> functionNames() {
    return List.of(new Object[][]{
        {"now", true, false},
        {"current_timestamp", true, true},
        {"localtimestamp", true, true},
        {"localtime", true, true},
        {"sysdate", true, false},
        {"curtime", true, false},
        {"current_time", true, true},
        {"curdate", false, false},
        {"current_date", false, true},
        {"utc_date", false, false},
        {"utc_time", false, false},
        {"utc_timestamp", false, false}
    });
  }

  private final String name;
  private final Boolean hasFsp;
  private final Boolean hasShortcut;

  @Test
  public void test_now_like_functions() {
    for (var call : hasShortcut ? List.of(name, name + "()") : List.of(name + "()")) {
      ParseTree tree = parser.parse("source=t | eval r=" + call);
      assertNotEquals(null, tree);

      tree = parser.parse("search source=t | where a=" + call);
      assertNotEquals(null, tree);
    }
    if (hasFsp) {
      ParseTree tree = parser.parse("search source=t | where a=" + name + "(0)");
      assertNotEquals(null, tree);
    }
  }
}

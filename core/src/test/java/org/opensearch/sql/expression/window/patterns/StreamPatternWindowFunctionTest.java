/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.patterns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;

public class StreamPatternWindowFunctionTest {

  private final CurrentRowWindowFrame windowFrame =
      new CurrentRowWindowFrame(new WindowDefinition(ImmutableList.of(), ImmutableList.of()));

  @Test
  void test_value_of() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(
                tuple(TEST_MESSAGE_1), tuple(TEST_MESSAGE_2), tuple(TEST_MESSAGE_3)));

    StreamPatternWindowFunction simplePattern =
        (StreamPatternWindowFunction) DSL.simple_pattern(DSL.ref("message", STRING));
    PatternsExpression patternsExpression =
        DSL.patterns(
            DSL.ref("message", STRING),
            new LiteralExpression(new ExprStringValue("")),
            new LiteralExpression(new ExprStringValue("")));

    windowFrame.load(tuples);
    assertEquals(
        patternsExpression.parseValue(new ExprStringValue(TEST_MESSAGE_1)),
        simplePattern.valueOf(windowFrame));
    windowFrame.load(tuples);
    assertEquals(
        patternsExpression.parseValue(new ExprStringValue(TEST_MESSAGE_2)),
        simplePattern.valueOf(windowFrame));
    windowFrame.load(tuples);
    assertEquals(
        patternsExpression.parseValue(new ExprStringValue(TEST_MESSAGE_3)),
        simplePattern.valueOf(windowFrame));
  }

  @Test
  void test_create_window_frame() {
    StreamPatternWindowFunction simplePattern =
        (StreamPatternWindowFunction) DSL.simple_pattern(DSL.ref("message", STRING));
    assertEquals(
        windowFrame,
        simplePattern.createWindowFrame(
            new WindowDefinition(ImmutableList.of(), ImmutableList.of())));
  }

  @Test
  void test_to_string() {
    StreamPatternWindowFunction simplePattern =
        (StreamPatternWindowFunction) DSL.simple_pattern(DSL.ref("message", STRING));
    assertEquals("simple_pattern(message)", simplePattern.toString());
  }

  private ExprValue tuple(String message) {
    return fromExprValueMap(ImmutableMap.of("message", new ExprStringValue(message)));
  }

  private static final String TEST_MESSAGE_1 =
      "12.132.31.17 - - [2018-07-22T05:36:25.812Z] \\\"GET /opensearch HTTP/1.1\\\" 200 9797"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421"
          + " Firefox/6.0a1\\\"";
  private static final String TEST_MESSAGE_2 =
      "129.138.185.193 - - [2018-07-22T05:39:39.668Z] \\\"GET /opensearch HTTP/1.1\\\" 404 9920"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421"
          + " Firefox/6.0a1\\\"";
  private static final String TEST_MESSAGE_3 =
      "240.58.187.246 - - [2018-07-22T06:02:46.006Z] \\\"GET /opensearch HTTP/1.1\\\" 500 6936"
          + " \\\"-\\\" \\\"Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.24 (KHTML, like Gecko)"
          + " Chrome/11.0.696.50 Safari/534.24\\\"";
  private static final BrainLogParser LOG_PARSER = new BrainLogParser();
}

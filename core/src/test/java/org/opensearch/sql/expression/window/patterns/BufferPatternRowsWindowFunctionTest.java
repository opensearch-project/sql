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
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.frame.BufferPatternRowsWindowFrame;

public class BufferPatternRowsWindowFunctionTest {

  private final BufferPatternRowsWindowFrame windowFrame =
      new BufferPatternRowsWindowFrame(
          new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
          LOG_PARSER,
          new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

  @Test
  void test_value_of() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(
                tuple(TEST_MESSAGE_1), tuple(TEST_MESSAGE_2), tuple(TEST_MESSAGE_3)));

    BufferPatternWindowFunction brain =
        (BufferPatternWindowFunction)
            DSL.brain(DSL.namedArgument("message", DSL.ref("message", STRING)));
    List<List<String>> preprocessedMessages =
        LOG_PARSER.preprocessAllLogs(Arrays.asList(TEST_MESSAGE_1, TEST_MESSAGE_2, TEST_MESSAGE_3));
    windowFrame.load(tuples);

    assertEquals(
        String.join(" ", LOG_PARSER.parseLogPattern(preprocessedMessages.get(0))),
        brain.valueOf(windowFrame).stringValue());
    assertEquals(
        String.join(" ", LOG_PARSER.parseLogPattern(preprocessedMessages.get(1))),
        brain.valueOf(windowFrame).stringValue());
    assertEquals(
        String.join(" ", LOG_PARSER.parseLogPattern(preprocessedMessages.get(2))),
        brain.valueOf(windowFrame).stringValue());
  }

  @Test
  void test_create_window_frame() {
    BufferPatternWindowFunction brain =
        (BufferPatternWindowFunction)
            DSL.brain(DSL.namedArgument("message", DSL.ref("message", STRING)));
    assertEquals(
        windowFrame,
        brain.createWindowFrame(new WindowDefinition(ImmutableList.of(), ImmutableList.of())));
  }

  @Test
  void test_to_string() {
    BufferPatternWindowFunction brain =
        (BufferPatternWindowFunction) DSL.brain(DSL.ref("message", STRING));
    assertEquals("brain(message)", brain.toString());
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

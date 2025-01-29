/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.window.WindowDefinition;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class BufferPatternRowsWindowFrameTest {

  private final BufferPatternRowsWindowFrame windowFrame =
      new BufferPatternRowsWindowFrame(
          new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
          LOG_PARSER,
          new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

  @Test
  void test_single_partition_for_all_rows() {
    PeekingIterator<ExprValue> tuples =
        Iterators.peekingIterator(
            Iterators.forArray(
                tuple(TEST_MESSAGE_1), tuple(TEST_MESSAGE_2), tuple(TEST_MESSAGE_3)));

    windowFrame.load(tuples);
    assertTrue(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_1, "0"), windowFrame.currentPreprocessedMessage());
    assertEquals(
        ImmutableList.of(tuple(TEST_MESSAGE_1), tuple(TEST_MESSAGE_2), tuple(TEST_MESSAGE_3)),
        windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_2, "1"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertTrue(windowFrame.hasNext());

    windowFrame.load(tuples);
    assertFalse(windowFrame.isNewPartition());
    assertEquals(
        LOG_PARSER.preprocess(TEST_MESSAGE_3, "2"), windowFrame.currentPreprocessedMessage());
    assertEquals(ImmutableList.of(), windowFrame.next());
    assertFalse(windowFrame.hasNext());
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

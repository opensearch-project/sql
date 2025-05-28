/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.frame.BufferPatternRowsWindowFrame;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;
import org.opensearch.sql.expression.window.frame.WindowFrame;

@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class PatternsWindowFunctionBenchmark {

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

  private PeekingIterator<ExprValue> tuples;
  private final BufferPatternRowsWindowFrame bufferWindowFrame =
      new BufferPatternRowsWindowFrame(
          new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
          new BrainLogParser(),
          new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

  @Benchmark
  public void testSimplePattern() {
    CurrentRowWindowFrame windowFrame =
        new CurrentRowWindowFrame(new WindowDefinition(ImmutableList.of(), ImmutableList.of()));

    run(windowFrame, DSL.simple_pattern(DSL.ref("message", STRING)));
  }

  @Benchmark
  public void testBrain() {
    BufferPatternRowsWindowFrame windowFrame =
        new BufferPatternRowsWindowFrame(
            new WindowDefinition(ImmutableList.of(), ImmutableList.of()),
            new BrainLogParser(),
            new NamedArgumentExpression("message", new ReferenceExpression("message", STRING)));

    run(windowFrame, DSL.brain(DSL.ref("message", STRING)));
  }

  private void run(WindowFrame windowFrame, Expression windowFunction) {
    tuples =
        Iterators.peekingIterator(
            Iterators.forArray(
                tuple(TEST_MESSAGE_1), tuple(TEST_MESSAGE_2), tuple(TEST_MESSAGE_3)));
    while (tuples.hasNext() || windowFrame.hasNext()) {
      windowFrame.load(tuples);
      windowFunction.valueOf(windowFrame);
    }
  }

  private ExprValue tuple(String message) {
    return fromExprValueMap(ImmutableMap.of("message", new ExprStringValue(message)));
  }
}

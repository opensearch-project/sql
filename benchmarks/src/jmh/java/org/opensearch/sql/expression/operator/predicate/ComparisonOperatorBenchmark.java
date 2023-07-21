/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.operator.predicate;

import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.expression.DSL.literal;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ComparisonOperatorBenchmark {

  @Param(value = { "int", "string", "date" })
  private String testDataType;

  private final Map<String, ExprValue> params =
      ImmutableMap.<String, ExprValue>builder()
          .put("int", integerValue(1))
          .put("string", stringValue("hello"))
          .put("date", fromObjectValue("2022-01-12", DATE))
          .build();

  @Benchmark
  public void testEqualOperator() {
    run(DSL::equal);
  }

  @Benchmark
  public void testLessOperator() {
    run(DSL::less);
  }

  @Benchmark
  public void testGreaterOperator() {
    run(DSL::greater);
  }

  private void run(Function<Expression[], FunctionExpression> dsl) {
    ExprValue param = params.get(testDataType);
    FunctionExpression func = dsl.apply(new Expression[] {
        literal(param), literal(param)
    });
    func.valueOf();
  }
}

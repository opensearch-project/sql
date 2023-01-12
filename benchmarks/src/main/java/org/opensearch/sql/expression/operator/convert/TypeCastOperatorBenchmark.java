/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.convert;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;

@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TypeCastOperatorBenchmark {

  @Param(value = { "2022-01-01"/*, "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"*/ })
  private String dateValue;

  @Param(value = { "2022-01-01 00:00:00"/*, "2022-01-02 00:00:00", "2022-01-02 00:00:00"*/ })
  private String dateTimeValue;

  private FunctionExpression function;

  @Setup
  public void init() {
    function = compileEqualFunction();
  }

  @Benchmark
  public boolean testDateEqualsDateTimeEvaluate() {
    ExprValue result = function.valueOf();
    return result.booleanValue();
  }

  @Benchmark
  public boolean testDateEqualsDateTimeCompileAndEvaluate() {
    FunctionExpression function = compileEqualFunction();
    ExprValue result = function.valueOf();
    return result.booleanValue();
  }

  private FunctionExpression compileEqualFunction() {
    return DSL.equal(
        DSL.literal(ExprValueUtils.fromObjectValue(dateValue, ExprCoreType.DATE)),
        DSL.literal(ExprValueUtils.fromObjectValue(dateTimeValue, ExprCoreType.DATETIME)));
  }
}

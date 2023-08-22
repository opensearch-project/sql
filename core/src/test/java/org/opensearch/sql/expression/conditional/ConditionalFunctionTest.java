/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.conditional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.env.Environment;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class ConditionalFunctionTest extends ExpressionTestBase {

  @Mock Environment<Expression, ExprValue> env;

  /** Arguments for case test. */
  private static Stream<Arguments> caseArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder
        .add(Arguments.of(18, 30, 45, 50, 60, 10))
        .add(Arguments.of(30, 30, 45, 50, 60, 10))
        .add(Arguments.of(50, 30, 45, 50, 60, 10))
        .build();
  }

  @ParameterizedTest(name = "case {0} when {1} then {2} when {3} then {4} else {5}")
  @MethodSource("caseArguments")
  void case_value(int value, int cond1, int result1, int cond2, int result2, int defaultVal)
      throws Exception {
    Callable<Integer> expect =
        () -> {
          if (cond1 == value) {
            return result1;
          } else if (cond2 == value) {
            return result2;
          } else {
            return defaultVal;
          }
        };

    Expression cases =
        DSL.cases(
            DSL.literal(defaultVal),
            DSL.when(DSL.equal(DSL.literal(cond1), DSL.literal(value)), DSL.literal(result1)),
            DSL.when(DSL.equal(DSL.literal(cond2), DSL.literal(value)), DSL.literal(result2)));

    assertEquals(new ExprIntegerValue(expect.call()), cases.valueOf(env));
  }
}

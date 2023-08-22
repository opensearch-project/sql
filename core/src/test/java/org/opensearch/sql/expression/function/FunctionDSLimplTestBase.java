/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

abstract class FunctionDSLimplTestBase extends FunctionDSLTestBase {
  @Test
  void implementationGenerator_is_valid() {
    assertNotNull(getImplementationGenerator());
  }

  @Test
  void implementation_is_valid_pair() {
    assertNotNull(getImplementation().getKey());
    assertNotNull(getImplementation().getValue());
  }

  @Test
  void implementation_expected_functionName() {
    assertEquals(SAMPLE_NAME, getImplementation().getKey().getFunctionName());
  }

  @Test
  void implementation_valid_functionBuilder() {

    FunctionBuilder v = getImplementation().getValue();
    assertDoesNotThrow(() -> v.apply(functionProperties, getSampleArguments()));
  }

  @Test
  void implementation_functionBuilder_return_functionExpression() {
    FunctionImplementation executable =
        getImplementation().getValue().apply(functionProperties, getSampleArguments());
    assertTrue(executable instanceof FunctionExpression);
  }

  @Test
  void implementation_functionExpression_valueOf() {
    FunctionExpression executable =
        (FunctionExpression)
            getImplementation().getValue().apply(functionProperties, getSampleArguments());

    assertEquals(ANY, executable.valueOf(null));
  }

  @Test
  void implementation_functionExpression_type() {
    FunctionExpression executable =
        (FunctionExpression)
            getImplementation().getValue().apply(functionProperties, getSampleArguments());
    assertEquals(ANY_TYPE, executable.type());
  }

  @Test
  void implementation_functionExpression_toString() {
    FunctionExpression executable =
        (FunctionExpression)
            getImplementation().getValue().apply(functionProperties, getSampleArguments());
    assertEquals(getExpected_toString(), executable.toString());
  }

  /** A lambda that takes a function name and returns an implementation of the function. */
  abstract SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      getImplementationGenerator();

  Pair<FunctionSignature, FunctionBuilder> getImplementation() {
    return getImplementationGenerator().apply(SAMPLE_NAME);
  }

  abstract List<Expression> getSampleArguments();

  abstract String getExpected_toString();
}

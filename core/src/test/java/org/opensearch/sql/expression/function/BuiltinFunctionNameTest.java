/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BuiltinFunctionNameTest {

  private static Stream<Arguments> ofArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return Arrays.asList(BuiltinFunctionName.values()).stream()
        .map(functionName -> Arguments.of(functionName.getName().getFunctionName(), functionName));
  }

  @ParameterizedTest
  @MethodSource("ofArguments")
  public void of(String name, BuiltinFunctionName expected) {
    assertTrue(BuiltinFunctionName.of(name).isPresent());
    assertEquals(expected, BuiltinFunctionName.of(name).get());
  }

  @Test
  public void caseInsensitive() {
    assertTrue(BuiltinFunctionName.of("aBs").isPresent());
    assertEquals(BuiltinFunctionName.of("aBs").get(), BuiltinFunctionName.ABS);
  }
}

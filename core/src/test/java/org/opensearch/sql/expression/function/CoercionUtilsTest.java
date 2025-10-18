/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;

class CoercionUtilsTest {

  private static Stream<Arguments> commonWidestTypeArguments() {
    return Stream.of(
        Arguments.of(STRING, INTEGER, DOUBLE),
        Arguments.of(INTEGER, STRING, DOUBLE),
        Arguments.of(STRING, DOUBLE, DOUBLE),
        Arguments.of(INTEGER, BOOLEAN, null));
  }

  @ParameterizedTest
  @MethodSource("commonWidestTypeArguments")
  public void findCommonWidestType(
      ExprCoreType left, ExprCoreType right, ExprCoreType expectedCommonType) {
    assertEquals(
        expectedCommonType, CoercionUtils.resolveCommonType(left, right).orElseGet(() -> null));
  }
}

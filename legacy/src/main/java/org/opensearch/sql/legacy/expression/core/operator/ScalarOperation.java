/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.operator;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** The definition of the Scalar Operation. */
@Getter
@RequiredArgsConstructor
public enum ScalarOperation {
  ADD("add"),
  SUBTRACT("subtract"),
  MULTIPLY("multiply"),
  DIVIDE("divide"),
  MODULES("modules"),
  ABS("abs"),
  ACOS("acos"),
  ASIN("asin"),
  ATAN("atan"),
  ATAN2("atan2"),
  TAN("tan"),
  CBRT("cbrt"),
  CEIL("ceil"),
  COS("cos"),
  COSH("cosh"),
  EXP("exp"),
  FLOOR("floor"),
  LN("ln"),
  LOG("log"),
  LOG2("log2"),
  LOG10("log10");

  private final String name;
}

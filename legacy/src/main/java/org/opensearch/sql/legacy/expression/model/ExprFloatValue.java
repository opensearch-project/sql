/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprFloatValue implements ExprValue {
  private final Float value;

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprValueKind kind() {
    return ExprValueKind.DOUBLE_VALUE;
  }

  @Override
  public String toString() {
    return value.toString();
  }
}

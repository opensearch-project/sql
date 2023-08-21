/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprBooleanValue implements ExprValue {
  private final Boolean value;

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprValueKind kind() {
    return ExprValueKind.BOOLEAN_VALUE;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("SSBooleanValue{");
    sb.append("value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}

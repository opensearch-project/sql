/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprTupleValue implements ExprValue {
  private final Map<String, ExprValue> valueMap;

  @Override
  public Object value() {
    return valueMap;
  }

  @Override
  public ExprValueKind kind() {
    return ExprValueKind.TUPLE_VALUE;
  }

  @Override
  public String toString() {
    return valueMap.entrySet().stream()
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(",", "{", "}"));
  }
}

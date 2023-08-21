/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

import static org.opensearch.sql.legacy.expression.model.ExprValue.ExprValueKind.COLLECTION_VALUE;

import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprCollectionValue implements ExprValue {
  private final List<ExprValue> valueList;

  @Override
  public Object value() {
    return valueList;
  }

  @Override
  public ExprValueKind kind() {
    return COLLECTION_VALUE;
  }

  @Override
  public String toString() {
    return valueList.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
  }
}

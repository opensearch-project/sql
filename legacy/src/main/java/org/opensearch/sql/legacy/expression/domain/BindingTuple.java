/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.domain;

import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import org.json.JSONObject;
import org.opensearch.sql.legacy.expression.model.ExprMissingValue;
import org.opensearch.sql.legacy.expression.model.ExprValue;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;

/**
 * BindingTuple represents the a relationship between bindingName and ExprValue. e.g. The operation
 * output column name is bindingName, the value is the ExprValue.
 */
@Builder
@Getter
@EqualsAndHashCode
public class BindingTuple {
  @Singular("binding")
  private final Map<String, ExprValue> bindingMap;

  /**
   * Resolve the Binding Name in BindingTuple context.
   *
   * @param bindingName binding name.
   * @return binding value.
   */
  public ExprValue resolve(String bindingName) {
    return bindingMap.getOrDefault(bindingName, new ExprMissingValue());
  }

  @Override
  public String toString() {
    return bindingMap.entrySet().stream()
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(",", "<", ">"));
  }

  public static BindingTuple from(Map<String, Object> map) {
    return from(new JSONObject(map));
  }

  public static BindingTuple from(JSONObject json) {
    Map<String, Object> map = json.toMap();
    BindingTupleBuilder bindingTupleBuilder = BindingTuple.builder();
    map.forEach((key, value) -> bindingTupleBuilder.binding(key, ExprValueFactory.from(value)));
    return bindingTupleBuilder.build();
  }
}

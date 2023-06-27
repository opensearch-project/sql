/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.utils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The Implementation of Content to represent {@link Object}.
 */
@RequiredArgsConstructor
public class ObjectContent implements Content {

  private final Object value;

  /**
   * The parse method parses the value as double value,
   * since the key values histogram buckets are defaulted to double.
   */
  @Override
  public Integer intValue() {
    return parseNumberValue(value, v -> Double.valueOf(v).intValue(), Number::intValue);
  }

  @Override
  public Long longValue() {
    return parseNumberValue(value, v -> Double.valueOf(v).longValue(), Number::longValue);
  }

  @Override
  public Short shortValue() {
    return parseNumberValue(value, v -> Double.valueOf(v).shortValue(), Number::shortValue);
  }

  @Override
  public Byte byteValue() {
    return parseNumberValue(value, v -> Double.valueOf(v).byteValue(), Number::byteValue);
  }

  @Override
  public Float floatValue() {
    return parseNumberValue(value, Float::valueOf, Number::floatValue);
  }

  @Override
  public Double doubleValue() {
    return parseNumberValue(value, Double::valueOf, Number::doubleValue);
  }

  @Override
  public String stringValue() {
    return (String) value;
  }

  @Override
  public Boolean booleanValue() {
    if (value instanceof String) {
      return Boolean.valueOf((String) value);
    } else if (value instanceof Number) {
      return ((Number) value).intValue() != 0;
    } else {
      return (Boolean) value;
    }
  }

  @Override
  public Object objectValue() {
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Map.Entry<String, Content>> map() {
    return ((Map<String, Object>) value).entrySet().stream()
        .map(entry -> (Map.Entry<String, Content>) new AbstractMap.SimpleEntry<String, Content>(
            entry.getKey(),
            new ObjectContent(entry.getValue())))
        .iterator();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<? extends Content> array() {
    return ((List<Object>) value).stream().map(ObjectContent::new).iterator();
  }

  @Override
  public boolean isNull() {
    return value == null;
  }

  @Override
  public boolean isNumber() {
    return value instanceof Number;
  }

  @Override
  public boolean isFloat() {
    return value instanceof Float;
  }

  @Override
  public boolean isDouble() {
    return value instanceof Double;
  }

  @Override
  public boolean isLong() {
    return value instanceof Long;
  }

  @Override
  public boolean isBoolean() {
    return value instanceof Boolean;
  }

  @Override
  public boolean isArray() {
    return value instanceof ArrayNode;
  }

  @Override
  public boolean isString() {
    return value instanceof String;
  }

  @Override
  public Pair<Double, Double> geoValue() {
    final String[] split = ((String) value).split(",");
    return Pair.of(Double.valueOf(split[0]), Double.valueOf(split[1]));
  }

  private <T> T parseNumberValue(Object value, Function<String, T> stringTFunction,
                                 Function<Number, T> numberTFunction) {
    if (value instanceof String) {
      return stringTFunction.apply((String) value);
    } else {
      return numberTFunction.apply((Number) value);
    }
  }
}

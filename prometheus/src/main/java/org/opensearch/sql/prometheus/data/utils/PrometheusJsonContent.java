/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.data.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The Implementation of Content to represent {@link JsonNode}.
 */
@RequiredArgsConstructor
public class PrometheusJsonContent implements Content {

  private final JsonNode value;

  @Override
  public Integer intValue() {
    return value().intValue();
  }

  @Override
  public Long longValue() {
    return value().longValue();
  }

  @Override
  public Short shortValue() {
    return value().shortValue();
  }

  @Override
  public Byte byteValue() {
    return (byte) value().shortValue();
  }

  @Override
  public Float floatValue() {
    return value().floatValue();
  }

  @Override
  public Double doubleValue() {
    return value().doubleValue();
  }

  @Override
  public String stringValue() {
    return value().asText();
  }

  @Override
  public Boolean booleanValue() {
    return value().booleanValue();
  }

  @Override
  public Iterator<Map.Entry<String, Content>> map() {
    LinkedHashMap<String, Content> map = new LinkedHashMap<>();
    final JsonNode mapValue = value();
    mapValue
        .fieldNames()
        .forEachRemaining(
            field -> map.put(field, new PrometheusJsonContent(mapValue.get(field))));
    return map.entrySet().iterator();
  }

  @Override
  public Iterator<? extends Content> array() {
    return Iterators.transform(value.elements(), PrometheusJsonContent::new);
  }

  @Override
  public boolean isNull() {
    return value == null || value.isNull() || (value.isArray() && value.isEmpty());
  }

  @Override
  public boolean isNumber() {
    return value().isNumber();
  }

  @Override
  public boolean isString() {
    return value().isTextual();
  }

  @Override
  public Object objectValue() {
    return value();
  }

  @Override
  public Pair<Double, Double> geoValue() {
    final JsonNode value = value();
    if (value.has("lat") && value.has("lon")) {
      Double lat = 0d;
      Double lon = 0d;
      try {
        lat = extractDoubleValue(value.get("lat"));
      } catch (Exception exception) {
        throw new IllegalStateException(
            "latitude must be number value, but got value: " + value.get(
                "lat"));
      }
      try {
        lon = extractDoubleValue(value.get("lon"));
      } catch (Exception exception) {
        throw new IllegalStateException(
            "longitude must be number value, but got value: " + value.get(
                "lon"));
      }
      return Pair.of(lat, lon);
    } else {
      throw new IllegalStateException("geo point must in format of {\"lat\": number, \"lon\": "
          + "number}");
    }
  }

  /**
   * Return the first element if is OpenSearch Array.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html.
   */
  private JsonNode value() {
    return value.isArray() ? value.get(0) : value;
  }

  /**
   * Get doubleValue from JsonNode if possible.
   */
  private Double extractDoubleValue(JsonNode node) {
    if (node.isTextual()) {
      return Double.valueOf(node.textValue());
    }
    if (node.isNumber()) {
      return node.doubleValue();
    } else {
      throw new IllegalStateException("node must be a number");
    }
  }
}

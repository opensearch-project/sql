/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.utils;


import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The Implementation of Content to represent {@link JsonElement}.
 */
@RequiredArgsConstructor
public class OpenSearchJsonContent implements Content {

  private final JsonElement value;

  @Override
  public Integer intValue() {
    return value().getAsInt();
  }

  @Override
  public Long longValue() {
    return value().getAsLong();
  }

  @Override
  public Short shortValue() {
    return value().getAsShort();
  }

  @Override
  public Byte byteValue() {
    return value().getAsByte();
  }

  @Override
  public Float floatValue() {
    return value().getAsFloat();
  }

  @Override
  public Double doubleValue() {
    return value().getAsDouble();
  }

  @Override
  public String stringValue() {
    return value().getAsString();
  }

  @Override
  public Boolean booleanValue() {
    return value().getAsBoolean();
  }

  @Override
  public Iterator<Map.Entry<String, Content>> map() {
    LinkedHashMap<String, Content> map = new LinkedHashMap<>();
    final JsonObject mapValue = value().getAsJsonObject();
    mapValue
        .keySet()
        .forEach(
            field -> map.put(field, new OpenSearchJsonContent(mapValue.get(field))));
    return map.entrySet().iterator();
  }

  @Override
  public Iterator<? extends Content> array() {
    return Iterators.transform(value.getAsJsonArray().iterator(), OpenSearchJsonContent::new);
  }

  @Override
  public boolean isNull() {
    return value == null || value.isJsonNull() 
      || (value.isJsonArray() && ((JsonArray) value).isEmpty());
  }

  @Override
  public boolean isNumber() {
    return value().isJsonPrimitive() && ((JsonPrimitive) value()).isNumber();
  }

  @Override
  public boolean isString() {
    return value().isJsonPrimitive() && ((JsonPrimitive) value()).isString();
  }

  @Override
  public Object objectValue() {
    return value();
  }

  @Override
  public Pair<Double, Double> geoValue() {
    final JsonObject value;
    try {
      value = value().getAsJsonObject();
    } catch (Exception e) {
      throw new IllegalStateException("geo point must in format of {\"lat\": number, \"lon\": "
          + "number}");
    }
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
  private JsonElement value() {
    return value.isJsonArray() ? ((JsonArray) value).get(0) : value;
  }

  /**
   * Get doubleValue from JsonNode if possible.
   */
  private Double extractDoubleValue(JsonElement node) {
    if (node.isJsonPrimitive() && node.getAsJsonPrimitive().isString()) {
      return Double.valueOf(node.getAsString());
    }
    if (node.isJsonPrimitive() && node.getAsJsonPrimitive().isNumber()) {
      return new Double(node.getAsDouble());
    } else {
      throw new IllegalStateException("node must be a number");
    }
  }
}

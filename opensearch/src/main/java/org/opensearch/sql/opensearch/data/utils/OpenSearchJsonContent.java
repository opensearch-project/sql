/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Numbers;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/** The Implementation of Content to represent {@link JsonNode}. */
@RequiredArgsConstructor
public class OpenSearchJsonContent implements Content {
  private static final Logger LOG = LogManager.getLogger();
  private final JsonNode value;

  @Override
  public Integer intValue() {
    return (int) parseLongValue(value());
  }

  @Override
  public Long longValue() {
    return parseLongValue(value());
  }

  @Override
  public Short shortValue() {
    return (short) parseLongValue(value());
  }

  @Override
  public Byte byteValue() {
    return (byte) parseLongValue(value());
  }

  @Override
  public Float floatValue() {
    return (float) parseDoubleValue(value());
  }

  @Override
  public Double doubleValue() {
    return parseDoubleValue(value());
  }

  @Override
  public String stringValue() {
    return value().asText();
  }

  @Override
  public Boolean booleanValue() {
    return parseBooleanValue(value());
  }

  @Override
  public Iterator<Map.Entry<String, Content>> map() {
    LinkedHashMap<String, Content> map = new LinkedHashMap<>();
    final JsonNode mapValue = value();
    mapValue
        .fieldNames()
        .forEachRemaining(field -> map.put(field, new OpenSearchJsonContent(mapValue.get(field))));
    return map.entrySet().iterator();
  }

  @Override
  public Iterator<? extends Content> array() {
    return Iterators.transform(value.elements(), OpenSearchJsonContent::new);
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
  public boolean isShort() {
    return value.isShort();
  }

  @Override
  public boolean isByte() {
    return false;
  }

  @Override
  public boolean isInt() {
    return value.isInt();
  }

  @Override
  public boolean isLong() {
    return value().isLong();
  }

  @Override
  public boolean isFloat() {
    return value().isFloat();
  }

  @Override
  public boolean isDouble() {
    return value().isDouble();
  }

  @Override
  public boolean isString() {
    return value().isTextual();
  }

  @Override
  public boolean isBoolean() {
    return value().isBoolean();
  }

  @Override
  public boolean isArray() {
    return value().isArray();
  }

  @Override
  public Object objectValue() {
    return value();
  }

  @Override
  public Pair<Double, Double> geoValue() {
    final JsonNode value = value();
    try (XContentParser parser =
        JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            value.toString())) {
      parser.nextToken();
      GeoPoint point = new GeoPoint();
      GeoUtils.parseGeoPoint(parser, point, true);
      return Pair.of(point.getLat(), point.getLon());
    } catch (IOException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error parsing geo point '{}'", value);
      }
      throw new OpenSearchParseException("error parsing geo point", ex);
    }
  }

  /** Getter for value. If value is array the whole array is returned. */
  private JsonNode value() {
    return value;
  }

  /** Parse long value from JsonNode. */
  private long parseLongValue(JsonNode node) {
    if (node.isNumber()) {
      return node.longValue();
    } else if (node.isTextual()) {
      if (node.textValue().isEmpty()) {
        return 0L;
      }
      return Numbers.toLong(node.textValue(), true);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("node '{}' must be a number", node);
      }
      throw new OpenSearchParseException(
          String.format("node must be a number, found %s", node.getNodeType()));
    }
  }

  /** Parse double value from JsonNode. */
  private double parseDoubleValue(JsonNode node) {
    if (node.isNumber()) {
      return node.doubleValue();
    } else if (node.isTextual()) {
      if (node.textValue().isEmpty()) {
        return 0.0;
      }
      return Double.parseDouble(node.textValue());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("node '{}' must be a number", node);
      }
      throw new OpenSearchParseException(
          String.format("node must be a number, found %s", node.getNodeType()));
    }
  }

  /** Parse boolean value from JsonNode. */
  private boolean parseBooleanValue(JsonNode node) {
    if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isTextual()) {
      return Boolean.parseBoolean(node.textValue());
    } else if (node.isNumber()) {
      return node.intValue() != 0;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("node '{}' must be a boolean", node);
      }
      throw new OpenSearchParseException(
          String.format("node must be a boolean, found %s", node.getNodeType()));
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.utils;

import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Regardless the underling data format, the {@link Content} define the data in abstract manner.
 * which could be parsed by ElasticsearchExprValueFactory.
 * There are two major use cases:
 * 1. Represent the JSON data retrieve from OpenSearch search response.
 * 2. Represent the Object data extract from the OpenSearch aggregation response.
 */
public interface Content {

  /**
   * Is null value.
   */
  boolean isNull();

  /**
   * Is number value.
   */
  boolean isNumber();

  /**
   * Is float value.
   */
  boolean isFloat();

  /**
   * Is double value.
   */
  boolean isDouble();

  /**
   * Is long value.
   */
  boolean isLong();

  /**
   * Is boolean value.
   */
  boolean isBoolean();

  /**
   * Is string value.
   */
  boolean isString();

  /**
   * Is array value.
   */
  boolean isArray();

  /**
   * Get integer value.
   */
  Integer intValue();

  /**
   * Get long value.
   */
  Long longValue();

  /**
   * Get short value.
   */
  Short shortValue();

  /**
   * Get byte value.
   */
  Byte byteValue();

  /**
   * Get float value.
   */
  Float floatValue();

  /**
   * Get double value.
   */
  Double doubleValue();

  /**
   * Get string value.
   */
  String stringValue();

  /**
   * Get boolean value.
   */
  Boolean booleanValue();

  /**
   * Get map of {@link Content} value.
   */
  Iterator<Map.Entry<String, Content>> map();

  /**
   * Get array of {@link Content} value.
   */
  Iterator<? extends Content> array();

  /**
   * Get geo point value.
   */
  Pair<Double, Double> geoValue();

  /**
   * Get {@link Object} value.
   */
  Object objectValue();
}

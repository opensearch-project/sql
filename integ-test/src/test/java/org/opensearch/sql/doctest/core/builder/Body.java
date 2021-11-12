/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.builder;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Request body.
 */
class Body {

  private final String[] fieldValues;

  /**
   * Request body built from field value pairs.
   *
   * @param fieldValues field and values in "'field": 'value'" format that can assemble to JSON directly
   */
  Body(String... fieldValues) {
    this.fieldValues = fieldValues;
  }

  @Override
  public String toString() {
    return Arrays.stream(fieldValues).collect(Collectors.joining(",", "{", "}"));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

/** Enum representing the types of resources that can be queried. */
public enum DirectQueryResourceType {
  UNKNOWN,
  LABELS,
  LABEL,
  METADATA,
  SERIES,
  ALERTS,
  RULES,
  ALERTMANAGER_ALERTS,
  ALERTMANAGER_ALERT_GROUPS,
  ALERTMANAGER_RECEIVERS,
  ALERTMANAGER_SILENCES;

  /**
   * Convert a string to the corresponding enum value, case-insensitive.
   *
   * @param value The string value to convert
   * @return The corresponding enum value
   * @throws IllegalArgumentException if the value doesn't match any enum value
   */
  public static DirectQueryResourceType fromString(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Resource type cannot be null");
    }

    try {
      return valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid resource type: " + value);
    }
  }
}

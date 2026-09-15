/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Locale;
import lombok.Value;

/**
 * A request-level time range, applying to every source the query reads. Bounds are kept as sent;
 * see {@code OpenSearchStorageEngine} for the formats accepted.
 */
@Value
public class TimeBounds {

  /** Not necessarily {@code @timestamp}. */
  String timeField;

  String start;
  String end;

  /**
   * @throws IllegalArgumentException if the field or either bound is blank
   */
  public TimeBounds(String timeField, String start, String end) {
    this.timeField = requireText(timeField, "time_field");
    this.start = requireText(start, "start_time");
    this.end = requireText(end, "end_time");
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must not be blank", name));
    }
    return value.trim();
  }
}

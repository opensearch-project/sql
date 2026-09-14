/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Locale;
import lombok.Value;

/**
 * A request-level time range, declared out of band from the query text and applying to every source
 * the query reads.
 *
 * <p>Bounds are kept as sent. OpenSearch date math and absolute timestamps are both accepted; which
 * formats an absolute bound may take is fixed by the probe, not the field's mapping. See {@code
 * IndexPruner}.
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

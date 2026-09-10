/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Locale;
import lombok.Value;

/**
 * A request-level time range: the window the caller is asking about, declared out of band from the
 * query text.
 *
 * <p>Global in scope, like Splunk's time range picker and ES|QL's request-level {@code filter} --
 * it applies to every source the query reads, subsearches included, rather than to one relation.
 * That is what lets it be applied before planning starts, which is the point: a wildcard index
 * expression is resolved, and every matched index's mapping merged, before any predicate in the
 * query text has been parsed. Nothing downstream of that resolution can narrow it.
 *
 * <p>Bounds are kept as the strings the request sent, not parsed here. OpenSearch date math ({@code
 * now-7d}) and absolute timestamps are both accepted, and both are evaluated by the same date
 * parser that reads the index's own mapping when the probe runs -- so there is no second
 * interpretation of a bound that could disagree with the first and silently exclude an index.
 *
 * @param timeField field the bounds constrain; the queried pattern's configured time field, which
 *     is not necessarily {@code @timestamp}
 * @param start inclusive lower bound
 * @param end inclusive upper bound
 */
@Value
public class TimeBounds {

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

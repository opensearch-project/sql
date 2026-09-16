/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Locale;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * A request-level time range, applying to every source the query reads. Bounds are kept as sent;
 * see {@code OpenSearchStorageEngine} for the formats accepted.
 *
 * <p>Reaches the storage engine encoded into the table name -- {@code logs-*} becomes {@code
 * logs-*<@timestamp,t0,t1>} -- as {@code SystemIndexUtils.restTable} does. {@code <} and {@code >}
 * are illegal in an index name, so an encoded name cannot collide with one a query could name.
 */
@Value
public class TimeBounds {

  private static final char OPEN = '<';
  private static final char CLOSE = '>';
  private static final char SEPARATOR = ',';

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

  /** A table name carrying these bounds, or {@code tableName} unchanged when they cannot be. */
  public String encodeInto(String tableName) {
    if (!isEncodable()) {
      return tableName;
    }
    return tableName + OPEN + timeField + SEPARATOR + start + SEPARATOR + end + CLOSE;
  }

  /** A table name and the bounds it carried, if any. */
  public record Decoded(String tableName, @Nullable TimeBounds bounds) {}

  /**
   * The name to read and the bounds to narrow it to. A name carrying none, or something unreadable,
   * decodes to itself.
   */
  public static Decoded decode(String tableName) {
    if (tableName == null
        || tableName.isEmpty()
        || tableName.charAt(tableName.length() - 1) != CLOSE) {
      return new Decoded(tableName, null);
    }
    int open = tableName.lastIndexOf(OPEN);
    if (open < 1) {
      return new Decoded(tableName, null);
    }
    String[] parts =
        tableName.substring(open + 1, tableName.length() - 1).split(String.valueOf(SEPARATOR), -1);
    if (parts.length != 3) {
      return new Decoded(tableName, null);
    }
    String name = tableName.substring(0, open);
    try {
      return new Decoded(name, new TimeBounds(parts[0], parts[1], parts[2]));
    } catch (IllegalArgumentException e) {
      return new Decoded(name, null);
    }
  }

  /** Whether a value can survive the round trip: the delimiters must not appear inside one. */
  private boolean isEncodable() {
    return isEncodable(timeField) && isEncodable(start) && isEncodable(end);
  }

  private static boolean isEncodable(String value) {
    return value.indexOf(OPEN) < 0 && value.indexOf(CLOSE) < 0 && value.indexOf(SEPARATOR) < 0;
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must not be blank", name));
    }
    return value.trim();
  }
}

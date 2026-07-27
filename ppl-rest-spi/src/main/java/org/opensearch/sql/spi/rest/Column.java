/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * One column of a {@code rest} endpoint's fixed output schema: a name, a {@link ColumnType}, and a
 * {@link RedactionClass} classifying the sensitivity of its values. The schema is fixed so the
 * query planner can pin the row type before execution. Column order is the output field order.
 *
 * <p>The redaction class is declared once here and reused by every endpoint that has such a column:
 * the platform registers a masker per class (OSS registers none), and the {@code rest} choke point
 * masks a cell only when its column class is not {@link RedactionClass#NONE} and a masker is
 * registered for that class. {@link #of(String, ColumnType)} defaults the class to {@code NONE}.
 */
public record Column(String name, ColumnType type, RedactionClass redaction) {

  public Column {
    redaction = redaction == null ? RedactionClass.NONE : redaction;
  }

  public static Column of(String name, ColumnType type) {
    return new Column(name, type, RedactionClass.NONE);
  }

  public static Column of(String name, ColumnType type, RedactionClass redaction) {
    return new Column(name, type, redaction);
  }
}

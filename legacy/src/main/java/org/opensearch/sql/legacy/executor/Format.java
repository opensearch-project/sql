/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Format {
  JDBC("jdbc"),
  JSON("json"),
  CSV("csv"),
  RAW("raw"),
  TABLE("table"),
  // format of explain response
  SIMPLE("simple"),
  STANDARD("standard"),
  EXTENDED("extended");

  @Getter private final String formatName;

  public static final Map<String, Format> RESPONSE_FORMATS;
  public static final Map<String, Format> EXPLAIN_FORMATS;

  static {
    ImmutableMap.Builder<String, Format> builder;
    builder = new ImmutableMap.Builder<>();
    builder.put(JDBC.formatName, JDBC);
    builder.put(CSV.formatName, CSV);
    builder.put(RAW.formatName, RAW);
    builder.put(TABLE.formatName, TABLE);
    RESPONSE_FORMATS = builder.build();

    builder = new ImmutableMap.Builder<>();
    builder.put(SIMPLE.formatName, SIMPLE);
    builder.put(STANDARD.formatName, STANDARD);
    builder.put(EXTENDED.formatName, EXTENDED);
    EXPLAIN_FORMATS = builder.build();
  }

  public static Optional<Format> of(String formatName) {
    return Optional.ofNullable(RESPONSE_FORMATS.getOrDefault(formatName, null));
  }

  public static Optional<Format> ofExplain(String formatName) {
    return Optional.ofNullable(EXPLAIN_FORMATS.getOrDefault(formatName, null));
  }
}

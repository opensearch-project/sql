/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Format {
  JDBC("jdbc"),
  CSV("csv"),
  RAW("raw"),
  VIZ("viz"),
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
    builder.put(VIZ.formatName, VIZ);
    RESPONSE_FORMATS = builder.build();

    builder = new ImmutableMap.Builder<>();
    builder.put(SIMPLE.formatName, SIMPLE);
    builder.put(STANDARD.formatName, STANDARD);
    builder.put(EXTENDED.formatName, EXTENDED);
    EXPLAIN_FORMATS = builder.build();
  }

  public static Optional<Format> of(String formatName) {
    String format = Strings.isNullOrEmpty(formatName) ? "jdbc" : formatName.toLowerCase();
    return Optional.ofNullable(RESPONSE_FORMATS.getOrDefault(format, null));
  }

  public static Optional<Format> ofExplain(String formatName) {
    String format = Strings.isNullOrEmpty(formatName) ? "standard" : formatName.toLowerCase();
    return Optional.ofNullable(EXPLAIN_FORMATS.getOrDefault(format, null));
  }
}

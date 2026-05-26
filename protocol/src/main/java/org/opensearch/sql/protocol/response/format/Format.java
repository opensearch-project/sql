/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Format {
  JDBC("jdbc"),
  CSV("csv"),
  RAW("raw"),
  VIZ("viz"),

  /*----- explain output format ------*/
  /** Returns explain output json format */
  JSON("json"),
  /** Returns explain output in yaml format */
  YAML("yaml"),

  /*---- backward compatible format of explain response -----*/
  SIMPLE("simple"),
  STANDARD("standard"),
  EXTENDED("extended"),
  COST("cost");

  @Getter private final String formatName;

  public static final Map<String, Format> RESPONSE_FORMATS;

  public static final Map<String, Format> EXPLAIN_FORMATS;

  public static final Set<Format> EXPLAIN_MODES;

  static {
    ImmutableMap.Builder<String, Format> builder;
    builder = new ImmutableMap.Builder<>();
    builder.put(JDBC.formatName, JDBC);
    builder.put(CSV.formatName, CSV);
    builder.put(RAW.formatName, RAW);
    builder.put(VIZ.formatName, VIZ);
    RESPONSE_FORMATS = builder.build();

    builder = new ImmutableMap.Builder<>();
    builder.put(JSON.formatName, JSON);
    builder.put(YAML.formatName, YAML);
    builder.put(SIMPLE.formatName, SIMPLE);
    builder.put(STANDARD.formatName, STANDARD);
    builder.put(EXTENDED.formatName, EXTENDED);
    builder.put(COST.formatName, COST);
    EXPLAIN_FORMATS = builder.build();

    EXPLAIN_MODES = Set.of(SIMPLE, STANDARD, EXTENDED, COST);
  }

  public static Optional<Format> of(String formatName) {
    String format = Strings.isNullOrEmpty(formatName) ? "jdbc" : formatName.toLowerCase();
    return Optional.ofNullable(RESPONSE_FORMATS.getOrDefault(format, null));
  }

  public static Optional<Format> ofExplain(String formatName) {
    String format = Strings.isNullOrEmpty(formatName) ? "json" : formatName.toLowerCase();
    return Optional.ofNullable(EXPLAIN_FORMATS.getOrDefault(format, null));
  }

  public static boolean isExplainMode(Format format) {
    return EXPLAIN_MODES.contains(format);
  }
}

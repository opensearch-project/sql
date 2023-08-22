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
  VIZ("viz");

  @Getter private final String formatName;

  private static final Map<String, Format> ALL_FORMATS;

  static {
    ImmutableMap.Builder<String, Format> builder = new ImmutableMap.Builder<>();
    for (Format format : Format.values()) {
      builder.put(format.formatName, format);
    }
    ALL_FORMATS = builder.build();
  }

  public static Optional<Format> of(String formatName) {
    String format = Strings.isNullOrEmpty(formatName) ? "jdbc" : formatName.toLowerCase();
    return Optional.ofNullable(ALL_FORMATS.getOrDefault(format, null));
  }
}

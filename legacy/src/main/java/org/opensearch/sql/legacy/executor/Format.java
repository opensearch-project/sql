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
  TABLE("table");

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
    return Optional.ofNullable(ALL_FORMATS.getOrDefault(formatName, null));
  }
}

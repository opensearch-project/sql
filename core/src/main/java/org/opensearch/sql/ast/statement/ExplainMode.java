/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import java.util.Locale;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ExplainMode {
  SIMPLE("simple"),
  STANDARD("standard"),
  EXTENDED("extended"),
  COST("cost");

  @Getter private final String modeName;

  public static ExplainMode of(String mode) {
    if (mode == null || mode.isEmpty()) return STANDARD;
    try {
      return ExplainMode.valueOf(mode.toUpperCase(Locale.ROOT));
    } catch (Exception e) {
      return ExplainMode.STANDARD;
    }
  }
}

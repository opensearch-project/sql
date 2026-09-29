/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import java.util.Locale;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlExplainLevel;

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

  /** Convert to Calcite SqlExplainLevel for RelOptUtil.toString(). */
  public SqlExplainLevel toExplainLevel() {
    return switch (this) {
      case SIMPLE -> SqlExplainLevel.NO_ATTRIBUTES;
      case COST -> SqlExplainLevel.ALL_ATTRIBUTES;
      default -> SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    };
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import lombok.Getter;

/**
 * Enumeration of query processing stages for error location tracking. These stages represent the
 * major phases of query execution in the Calcite query planner. May not be a complete list, add
 * stages if needed.
 */
@Getter
public enum QueryProcessingStage {
  /**
   * ANALYZING stage: Semantic validation and type checking. Errors: Field not found, type
   * mismatches, semantic violations.
   */
  ANALYZING("Parsing and validating the query"),

  /**
   * PLAN_CONVERSION stage: Conversion to Calcite execution plan with system limits. Errors:
   * Unsupported operations, plan conversion failures.
   */
  PLAN_CONVERSION("Preparing the query for physical execution"),

  /**
   * EXECUTING stage: Query execution via OpenSearch engine. Errors: Execution failures, index
   * access errors, resource limits.
   */
  EXECUTING("Running the query");

  /** -- GETTER -- Get human-readable display name for this stage. */
  private final String displayName;

  QueryProcessingStage(String displayName) {
    this.displayName = displayName;
  }

  /** Get lowercase name suitable for JSON serialization. */
  public String toJsonKey() {
    return name().toLowerCase();
  }
}

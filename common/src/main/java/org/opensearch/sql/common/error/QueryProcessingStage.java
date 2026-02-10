/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

/**
 * Enumeration of query processing stages for error location tracking. These stages represent the
 * major phases of query execution in the Calcite query planner.
 *
 * <p>Stage progression for a typical query: PARSING → AST_BUILDING → ANALYZING → OPTIMIZING →
 * PLAN_CONVERSION → EXECUTING → RESULT_PROCESSING
 */
public enum QueryProcessingStage {
  /**
   * PARSING stage: PPL/SQL syntax parsing via ANTLR lexer/parser. Errors: Syntax errors, unexpected
   * tokens, malformed queries.
   */
  PARSING("Validating Query Syntax"),

  /**
   * AST_BUILDING stage: Abstract Syntax Tree construction from parse tree. Errors: Invalid AST
   * structure, node creation failures.
   */
  AST_BUILDING("Building Query Structure"),

  /**
   * ANALYZING stage: Semantic validation and type checking. Errors: Field not found, type
   * mismatches, semantic violations.
   */
  ANALYZING("Checking Query Against Your Data"),

  /**
   * OPTIMIZING stage: Logical plan optimization with transformation rules. Errors: Optimization
   * failures, rule application errors.
   */
  OPTIMIZING("Optimizing Query"),

  /**
   * PLAN_CONVERSION stage: Conversion to Calcite execution plan with system limits. Errors:
   * Unsupported operations, plan conversion failures.
   */
  PLAN_CONVERSION("Preparing Query Execution"),

  /**
   * EXECUTING stage: Query execution via OpenSearch engine. Errors: Execution failures, index
   * access errors, resource limits.
   */
  EXECUTING("Running Query on Cluster"),

  /**
   * RESULT_PROCESSING stage: Result formatting and cursor management. Errors: Result formatting
   * failures, cursor errors.
   */
  RESULT_PROCESSING("Formatting Results"),

  /**
   * CLUSTER_VALIDATION stage: Prevalidation that cluster is healthy before execution. Errors:
   * Cluster unhealthy, nodes unavailable.
   */
  CLUSTER_VALIDATION("Checking Cluster Health");

  private final String displayName;

  QueryProcessingStage(String displayName) {
    this.displayName = displayName;
  }

  /** Get human-readable display name for this stage. */
  public String getDisplayName() {
    return displayName;
  }

  /** Get lowercase name suitable for JSON serialization. */
  public String toJsonKey() {
    return name().toLowerCase();
  }
}

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
  PARSING("Validating query syntax"),

  /**
   * AST_BUILDING stage: Abstract Syntax Tree construction from parse tree. Errors: Invalid AST
   * structure, node creation failures.
   */
  AST_BUILDING("Assembling the query steps"),

  /**
   * ANALYZING stage: Semantic validation and type checking. Errors: Field not found, type
   * mismatches, semantic violations.
   */
  ANALYZING("Validating the query's steps"),

  /**
   * OPTIMIZING stage: Logical plan optimization with transformation rules. Errors: Optimization
   * failures, rule application errors.
   */
  OPTIMIZING("Optimizing the query"),

  /**
   * PLAN_CONVERSION stage: Conversion to Calcite execution plan with system limits. Errors:
   * Unsupported operations, plan conversion failures.
   */
  PLAN_CONVERSION("Preparing the query for physical execution"),

  /**
   * EXECUTING stage: Query execution via OpenSearch engine. Errors: Execution failures, index
   * access errors, resource limits.
   */
  EXECUTING("Running the query"),

  /**
   * RESULT_PROCESSING stage: Result formatting and cursor management. Errors: Result formatting
   * failures, cursor errors.
   */
  RESULT_PROCESSING("Processing the query results"),

  /**
   * CLUSTER_VALIDATION stage: Prevalidation that cluster is healthy before execution. Errors:
   * Cluster unhealthy, nodes unavailable.
   */
  CLUSTER_VALIDATION("Checking cluster health");

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

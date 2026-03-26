/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

/**
 * Machine-readable error codes for categorizing exceptions. These codes help clients handle
 * specific error types programmatically. <br>
 * <br>
 * Not a complete list, currently seeded with some initial values. Feel free to add variants or
 * remove dead variants over time.
 */
public enum ErrorCode {
  /** Field not found in the index mapping */
  FIELD_NOT_FOUND,

  /** Syntax error in query parsing */
  SYNTAX_ERROR,

  /** Ambiguous field reference (multiple fields with same name) */
  AMBIGUOUS_FIELD,

  /** Generic semantic validation error */
  SEMANTIC_ERROR,

  /** Expression evaluation failed */
  EVALUATION_ERROR,

  /** Type mismatch or type validation error */
  TYPE_ERROR,

  /** Unsupported feature or operation */
  UNSUPPORTED_OPERATION,

  /** Resource limit exceeded (memory, CPU, etc.) */
  RESOURCE_LIMIT_EXCEEDED,

  /** Index or datasource not found */
  INDEX_NOT_FOUND,

  /** Query planning failed */
  PLANNING_ERROR,

  /** Query execution failed */
  EXECUTION_ERROR,

  /**
   * Unknown or unclassified error -- don't set this manually, it's filled in as the default if no
   * other code applies.
   */
  UNKNOWN
}

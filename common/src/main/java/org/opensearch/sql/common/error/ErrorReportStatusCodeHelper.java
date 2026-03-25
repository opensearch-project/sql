/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import java.util.function.Predicate;

/**
 * Helper class for determining HTTP status codes from ErrorReport instances. Provides utilities to
 * check if an exception (potentially wrapped in ErrorReport) should result in a client error (4xx)
 * or server error (5xx) status code.
 */
public class ErrorReportStatusCodeHelper {

  /**
   * Determines if an exception should be treated as a client error (400 BAD_REQUEST) based on:
   *
   * <ol>
   *   <li>ErrorCode (if wrapped in ErrorReport)
   *   <li>Underlying exception type (checked via provided predicate)
   * </ol>
   *
   * @param e the exception to check
   * @param exceptionTypeChecker predicate that checks if a given exception type is a client error
   * @return true if this should result in a 4xx status code, false for 5xx
   */
  public static boolean isClientError(Exception e, Predicate<Exception> exceptionTypeChecker) {
    // If wrapped in ErrorReport, check both the ErrorCode and the underlying cause
    if (e instanceof ErrorReport) {
      ErrorReport report = (ErrorReport) e;

      // Check if the ErrorCode indicates a client error
      if (isClientErrorCode(report.getCode())) {
        return true;
      }

      // Fall through to check the underlying exception type
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        return exceptionTypeChecker.test((Exception) cause);
      }
    }

    // Check the exception type directly
    return exceptionTypeChecker.test(e);
  }

  /**
   * Maps ErrorCode values to client error (400) vs server error (500) status codes.
   *
   * <p>Client errors (400):
   *
   * <ul>
   *   <li>FIELD_NOT_FOUND - invalid field reference
   *   <li>SYNTAX_ERROR - query parsing failed
   *   <li>AMBIGUOUS_FIELD - multiple fields with same name
   *   <li>SEMANTIC_ERROR - semantic validation failed
   *   <li>TYPE_ERROR - type mismatch
   *   <li>UNSUPPORTED_OPERATION - feature not supported
   *   <li>INDEX_NOT_FOUND - index doesn't exist
   * </ul>
   *
   * <p>Server errors (500):
   *
   * <ul>
   *   <li>EVALUATION_ERROR - runtime evaluation failed
   *   <li>PLANNING_ERROR - query planning failed
   *   <li>EXECUTION_ERROR - query execution failed
   *   <li>RESOURCE_LIMIT_EXCEEDED - system resource limit
   *   <li>UNKNOWN - unclassified error
   * </ul>
   *
   * @param code the ErrorCode to check
   * @return true if this ErrorCode indicates a client error (400), false for server error (500)
   */
  private static boolean isClientErrorCode(ErrorCode code) {
    if (code == null) {
      return false;
    }

    switch (code) {
      case FIELD_NOT_FOUND:
      case SYNTAX_ERROR:
      case AMBIGUOUS_FIELD:
      case SEMANTIC_ERROR:
      case TYPE_ERROR:
      case UNSUPPORTED_OPERATION:
      case INDEX_NOT_FOUND:
        return true;

      case EVALUATION_ERROR:
      case PLANNING_ERROR:
      case EXECUTION_ERROR:
      case RESOURCE_LIMIT_EXCEEDED:
      case UNKNOWN:
      default:
        return false;
    }
  }
}

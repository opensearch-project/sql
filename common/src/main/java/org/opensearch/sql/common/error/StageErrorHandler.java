/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import java.util.function.Supplier;

/**
 * Utility class for handling errors at specific query processing stages. This provides a consistent
 * way to wrap operations with stage-specific error context.
 *
 * <p>Example usage in QueryService:
 *
 * <pre>
 * RelNode relNode = StageErrorHandler.executeStage(
 *   QueryProcessingStage.ANALYZING,
 *   () -> analyze(plan, context),
 *   "while analyzing query plan"
 * );
 * </pre>
 */
public class StageErrorHandler {

  /**
   * Execute an operation and wrap any thrown exceptions with stage context.
   *
   * @param stage The query processing stage
   * @param operation The operation to execute
   * @param location Optional location description for error context
   * @param <T> Return type of the operation
   * @return The result of the operation
   * @throws ErrorReport if the operation throws an exception
   */
  public static <T> T executeStage(
      QueryProcessingStage stage, Supplier<T> operation, String location) {
    try {
      return operation.get();
    } catch (ErrorReport e) {
      // If already an ErrorReport, just add stage if not set
      throw ErrorReport.wrap(e).stage(stage).location(location).build();
    } catch (Exception e) {
      throw ErrorReport.wrap(e).stage(stage).location(location).build();
    }
  }

  /**
   * Execute an operation and wrap any thrown exceptions with stage context (no location).
   *
   * @param stage The query processing stage
   * @param operation The operation to execute
   * @param <T> Return type of the operation
   * @return The result of the operation
   * @throws ErrorReport if the operation throws an exception
   */
  public static <T> T executeStage(QueryProcessingStage stage, Supplier<T> operation) {
    return executeStage(stage, operation, null);
  }

  /**
   * Execute a void operation and wrap any thrown exceptions with stage context.
   *
   * @param stage The query processing stage
   * @param operation The operation to execute
   * @param location Optional location description for error context
   * @throws ErrorReport if the operation throws an exception
   */
  public static void executeStageVoid(
      QueryProcessingStage stage, Runnable operation, String location) {
    try {
      operation.run();
    } catch (ErrorReport e) {
      throw ErrorReport.wrap(e).stage(stage).location(location).build();
    } catch (Exception e) {
      throw ErrorReport.wrap(e).stage(stage).location(location).build();
    }
  }

  /**
   * Execute a void operation and wrap any thrown exceptions with stage context (no location).
   *
   * @param stage The query processing stage
   * @param operation The operation to execute
   * @throws ErrorReport if the operation throws an exception
   */
  public static void executeStageVoid(QueryProcessingStage stage, Runnable operation) {
    executeStageVoid(stage, operation, null);
  }

  /**
   * Wrap an exception with stage context without executing an operation. Useful for re-throwing
   * exceptions with additional context.
   *
   * @param stage The query processing stage
   * @param e The exception to wrap
   * @param location Optional location description
   * @return ErrorReport with stage context
   */
  public static ErrorReport wrapWithStage(
      QueryProcessingStage stage, Throwable e, String location) {
    ErrorReport.Builder builder = ErrorReport.wrap(e).stage(stage);
    if (location != null) {
      builder.location(location);
    }
    return builder.build();
  }
}

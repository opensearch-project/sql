/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.exception.ExceptionUtils;

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

  private static final String CODEGEN_FAILURE_REASON =
      "Internal error while compiling the query plan.";

  private static final String CODEGEN_FAILURE_DETAILS =
      "The engine generated invalid code for this query plan. This is an engine defect, not a"
          + " problem with the query. The compiler error is in the OpenSearch node log.";

  // Matched by class name because janino is runtime-only here. It arrives through calcite-core and
  // is absent from the compile classpath, so importing CompileException would not compile and
  // instanceof is unavailable.
  //
  // The class check alone also passes on a single-node cluster and fails on a real one.
  // StreamOutput.writeException swaps any throwable that is not a registered OpenSearchException
  // for a NotSerializableExceptionWrapper, whose message is getExceptionName(other) + ": " +
  // other.getMessage() with the name snake-cased. So only message text crosses a transport hop,
  // and the prefix below is what CompileException becomes on the far side.
  private static final String COMPILE_EXCEPTION_CLASS = "CompileException";

  private static final String SERIALIZED_COMPILE_EXCEPTION_PREFIX = "compile_exception: ";

  private static final String[] CODEGEN_MESSAGE_MARKERS = {
    "While compiling generated Rex code",
    "Failed to compile inline script",
    "Error while compiling generated Java code",
    // Carries its separator so a user-supplied identifier holding the bare type name cannot match.
    SERIALIZED_COMPILE_EXCEPTION_PREFIX
  };

  /**
   * The compile exception itself, when its concrete class survived, or null. Only its message is
   * safe to surface, since the wrappers around it embed the whole generated class body in theirs.
   */
  private static Throwable findCompileException(List<Throwable> chain) {
    return chain.stream()
        .filter(t -> t.getClass().getSimpleName().equals(COMPILE_EXCEPTION_CLASS))
        .findFirst()
        .orElse(null);
  }

  /** Whether anything in the chain marks this as a codegen failure, by class or by message. */
  private static boolean isCodegenFailure(List<Throwable> chain) {
    return chain.stream().anyMatch(StageErrorHandler::marksCodegen);
  }

  private static boolean marksCodegen(Throwable t) {
    if (t.getClass().getSimpleName().equals(COMPILE_EXCEPTION_CLASS)) {
      return true;
    }
    String message = t.getMessage();
    return message != null && Arrays.stream(CODEGEN_MESSAGE_MARKERS).anyMatch(message::contains);
  }

  /**
   * The compiler diagnostic from a serialized chain, or null. The text behind the wrapper's prefix
   * is the bare diagnostic, so it is safe to surface. The other markers are not.
   */
  private static String serializedCompilerMessage(List<Throwable> chain) {
    return chain.stream()
        .map(Throwable::getMessage)
        .filter(m -> m != null && m.startsWith(SERIALIZED_COMPILE_EXCEPTION_PREFIX))
        .map(m -> m.substring(SERIALIZED_COMPILE_EXCEPTION_PREFIX.length()))
        .findFirst()
        .orElse(null);
  }

  /**
   * Replace the engine internals with a generic reason on a codegen defect. Without this the
   * details default to the wrapped exception's message, which on the shard-level script path is a
   * nested OpenSearch chain carrying a node address and the serialized expression tree.
   */
  private static ErrorReport.Builder hideCodegenInternals(
      QueryProcessingStage stage, ErrorReport.Builder builder, Throwable e) {
    // Code generation only runs under EXECUTING. Gating on the stage stops a user-supplied
    // identifier that happens to carry a marker from reading as an engine defect, since a field
    // name reaches the analysis stages inside the message.
    if (stage != QueryProcessingStage.EXECUTING || builder.hasReason()) {
      return builder;
    }
    // getThrowableList halts if the cause chain loops back on itself. Do not swap in Guava's
    // getCausalChain, which throws on that, and this runs inside a catch block where a new throw
    // would replace the user's error. Taken once, since both checks read the same chain.
    List<Throwable> chain = ExceptionUtils.getThrowableList(e);
    if (!isCodegenFailure(chain)) {
      return builder;
    }
    builder.reason(CODEGEN_FAILURE_REASON);

    Throwable compileException = findCompileException(chain);
    String serialized = serializedCompilerMessage(chain);
    if (compileException != null && compileException.getMessage() != null) {
      builder.details(compileException.getMessage());
    } else if (serialized != null) {
      builder.details(serialized);
    } else {
      // Detected by a marker whose own message embeds the generated source, so nothing in the
      // chain is known to be free of engine internals.
      builder.details(CODEGEN_FAILURE_DETAILS);
    }
    return builder;
  }

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
    } catch (Exception e) {
      throw hideCodegenInternals(stage, ErrorReport.wrap(e).stage(stage).location(location), e)
          .build();
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
    } catch (Exception e) {
      throw hideCodegenInternals(stage, ErrorReport.wrap(e).stage(stage).location(location), e)
          .build();
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
      QueryProcessingStage stage, Exception e, String location) {
    ErrorReport.Builder builder = ErrorReport.wrap(e).stage(stage);
    if (location != null) {
      builder.location(location);
    }
    return hideCodegenInternals(stage, builder, e).build();
  }
}

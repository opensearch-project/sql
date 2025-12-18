/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.compiler;

import java.sql.PreparedStatement;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/**
 * {@code UnifiedQueryCompiler} compiles Calcite logical plans ({@link RelNode}) into executable
 * JDBC statements, separating query compilation from execution.
 */
public class UnifiedQueryCompiler {

  /** Unified query context containing CalcitePlanContext with all configuration. */
  private final UnifiedQueryContext context;

  /**
   * Constructs a UnifiedQueryCompiler with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryCompiler(UnifiedQueryContext context) {
    this.context = context;
  }

  /**
   * Compiles a Calcite logical plan into an executable PreparedStatement.
   *
   * @param plan the logical plan to compile (must not be null)
   * @return a compiled PreparedStatement ready for execution
   * @throws IllegalStateException if compilation fails
   */
  public PreparedStatement compile(@NonNull RelNode plan) {
    try {
      return CalciteToolsHelper.OpenSearchRelRunners.run(context.getPlanContext(), plan);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to compile logical plan", e);
    }
  }
}

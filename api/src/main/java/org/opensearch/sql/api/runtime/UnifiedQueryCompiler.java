/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.runtime;

import java.sql.PreparedStatement;
import lombok.Builder;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/**
 * {@code UnifiedQueryCompiler} compiles Calcite logical plans ({@link RelNode}) into executable
 * JDBC statements, separating query compilation from execution.
 */
@Builder
public class UnifiedQueryCompiler {

  /** The unified query context containing Calcite configuration and settings. */
  private final UnifiedQueryContext context;

  /**
   * Compiles a Calcite logical plan into an executable PreparedStatement.
   *
   * <p>The returned PreparedStatement can be executed multiple times and should be closed when
   * done. Results are returned as standard JDBC ResultSet.
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.compiler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import lombok.NonNull;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelRunner;
import org.opensearch.sql.api.UnifiedQueryContext;

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
   * Compiles a Calcite logical plan into an executable {@link PreparedStatement}. Similar to {@code
   * CalciteToolsHelper.OpenSearchRelRunners.run()} but does not close the connection, leaving
   * resource management to {@link UnifiedQueryContext}.
   *
   * @param plan the logical plan to compile (must not be null)
   * @return a compiled PreparedStatement ready for execution
   * @throws IllegalStateException if compilation fails
   */
  public PreparedStatement compile(@NonNull RelNode plan) {
    try {
      // Apply shuttle to convert LogicalTableScan to BindableTableScan
      final RelHomogeneousShuttle shuttle =
          new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
              final RelOptTable table = scan.getTable();
              if (scan instanceof LogicalTableScan
                  && Bindables.BindableTableScan.canHandle(table)) {
                return Bindables.BindableTableScan.create(scan.getCluster(), table);
              }
              return super.visit(scan);
            }
          };
      RelNode transformedPlan = plan.accept(shuttle);

      Connection connection = context.getPlanContext().connection;
      final RelRunner runner = connection.unwrap(RelRunner.class);
      return runner.prepareStatement(transformedPlan);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to compile logical plan", e);
    }
  }
}

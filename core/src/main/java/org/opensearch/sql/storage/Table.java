/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import java.util.Map;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Table.
 */
public interface Table {

  /**
   * Check if current table exists.
   *
   * @return true if exists, otherwise false
   */
  default boolean exists() {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  /**
   * Create table given table schema.
   *
   * @param schema table schema
   */
  default void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  /**
   * Get the {@link ExprType} for each field in the table.
   */
  Map<String, ExprType> getFieldTypes();

  /**
   * Implement a {@link LogicalPlan} by {@link PhysicalPlan} in storage engine.
   *
   * @param plan logical plan
   * @return physical plan
   * @deprecated replaced by write/scan builder
   */
  @Deprecated(since = "2.5")
  PhysicalPlan implement(LogicalPlan plan);

  /**
   * Optimize the {@link LogicalPlan} by storage engine rule.
   * The default optimize solution is no optimization.
   *
   * @param plan logical plan.
   * @return logical plan.
   * @deprecated replaced by write/scan builder
   */
  @Deprecated(since = "2.5")
  default LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

  /**
   * Create table scan builder for logical to physical transformation.
   * @return table scan builder
   */
  default TableScanBuilder createScanBuilder() {
    return null; // TODO: enforce all connector to create one for unified optimizing workflow
  }

}

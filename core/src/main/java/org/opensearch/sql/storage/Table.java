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
   * Create table given table schema.
   * @param schema table schema
   * @return true if created successfully, otherwise false
   */
  default boolean create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "Create table is not supported in the storage engine");
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
   */
  PhysicalPlan implement(LogicalPlan plan);

  /**
   * Optimize the {@link LogicalPlan} by storage engine rule.
   * The default optimize solution is no optimization.
   *
   * @param plan logical plan.
   * @return logical plan.
   */
  default LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

}

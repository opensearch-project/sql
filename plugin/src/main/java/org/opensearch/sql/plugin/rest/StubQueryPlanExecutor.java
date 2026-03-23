/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import java.sql.Timestamp;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;

/**
 * Stub implementation of {@link QueryPlanExecutor} for development and testing. Returns canned data
 * so the full pipeline (routing → planning → execution → response formatting) can be validated
 * without the analytics engine.
 *
 * <p>Will be replaced by the real analytics engine implementation when available.
 */
public class StubQueryPlanExecutor implements QueryPlanExecutor {

  @Override
  public Iterable<Object[]> execute(RelNode plan, Object context) {
    // Return canned rows matching the stub schema defined in RestUnifiedQueryAction.
    // The column order must match the schema: ts, status, message, ip_addr
    // (for parquet_logs table). For other tables, return empty results.
    String tableName = extractTableName(plan);
    if (tableName != null && tableName.contains("parquet_logs")) {
      return List.of(
          new Object[] {
            Timestamp.valueOf("2024-01-15 10:30:00"), 200, "Request completed", "192.168.1.1"
          },
          new Object[] {
            Timestamp.valueOf("2024-01-15 10:31:00"), 200, "Health check OK", "192.168.1.2"
          },
          new Object[] {
            Timestamp.valueOf("2024-01-15 10:32:00"), 500, "Internal server error", "192.168.1.3"
          });
    }
    if (tableName != null && tableName.contains("parquet_metrics")) {
      return List.of(
          new Object[] {Timestamp.valueOf("2024-01-15 10:30:00"), 75.5, 8192.0, "host-1"},
          new Object[] {Timestamp.valueOf("2024-01-15 10:31:00"), 82.3, 7680.0, "host-2"});
    }
    return List.of();
  }

  private String extractTableName(RelNode plan) {
    // Use RelOptUtil.toString to get the full plan tree including child nodes
    String planStr = RelOptUtil.toString(plan);
    if (planStr.contains("parquet_logs")) {
      return "parquet_logs";
    }
    if (planStr.contains("parquet_metrics")) {
      return "parquet_metrics";
    }
    return null;
  }
}

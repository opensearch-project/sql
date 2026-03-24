/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import org.apache.calcite.rel.RelNode;

/**
 * Executes a Calcite {@link RelNode} logical plan against the analytics engine.
 *
 * <p>This is a local equivalent of {@code org.opensearch.analytics.exec.QueryPlanExecutor} from the
 * analytics-framework library. It will be replaced by the upstream interface once the
 * analytics-framework JAR is published.
 *
 * @see <a
 *     href="https://github.com/opensearch-project/OpenSearch/blob/9142d0e789c6a6c4708f1bc015745ed55202eefe/sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/QueryPlanExecutor.java">Upstream
 *     QueryPlanExecutor</a>
 */
@FunctionalInterface
public interface QueryPlanExecutor {

  /**
   * Executes the given logical plan and returns result rows.
   *
   * @param plan the Calcite RelNode subtree to execute
   * @param context execution context (opaque to avoid server dependency)
   * @return rows produced by the engine
   */
  Iterable<Object[]> execute(RelNode plan, Object context);
}

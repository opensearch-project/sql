/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;

/**
 * Bridge for sharing the analytics-engine {@link QueryPlanExecutor} between the PPL transport
 * action (where Guice resolves the binding via {@code @Inject}) and the REST-only SQL router (where
 * Guice cannot, because {@code SQLPlugin#getRestHandlers} runs before the Node-level injector
 * satisfies {@code @Inject} parameters).
 *
 * <p>Why a static holder: cross-plugin Guice injection needs a class registered in the Node
 * injector, and {@link org.opensearch.sql.plugin.SQLPlugin}'s SQL routing path is built in {@code
 * getRestHandlers} — outside any Guice-managed lifecycle. Persisting the executor in this holder
 * once {@link org.opensearch.sql.plugin.transport.TransportPPLQueryAction} is constructed lets the
 * SQL router read the same instance without going back through the injector.
 */
public final class AnalyticsExecutorHolder {

  private static volatile QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;

  private AnalyticsExecutorHolder() {}

  public static void set(QueryPlanExecutor<RelNode, Iterable<Object[]>> instance) {
    executor = instance;
  }

  public static QueryPlanExecutor<RelNode, Iterable<Object[]>> get() {
    return executor;
  }
}

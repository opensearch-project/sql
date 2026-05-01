/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

/**
 * Static bridge that lets {@code SQLAnalyticsFrontEndExtension} (the SPI consumer) hand off the
 * analytics-engine services to the SQL request paths that need them.
 *
 * <p>Stored as {@link Object} on purpose. Callers cast at use sites that are already gated on a
 * non-null value, ensuring no analytics-framework class is referenced from any signature loaded at
 * SQL plugin startup. When analytics-engine is not installed, the SPI never fires, the holder stays
 * null, and {@code TransportPPLQueryAction} / {@code SQLPlugin#createSqlAnalyticsRouter} fall
 * through to the legacy paths without ever touching analytics-framework types.
 */
public final class AnalyticsExecutorHolder {

  private static volatile Object queryPlanExecutor;
  private static volatile Object schemaProvider;

  private AnalyticsExecutorHolder() {}

  /**
   * Set both services in one call. Invoked by {@code SQLAnalyticsFrontEndExtension} from the SPI
   * push lifecycle. Either argument may be {@code null} (not expected today, but tolerated).
   */
  public static void set(Object queryPlanExecutor, Object schemaProvider) {
    AnalyticsExecutorHolder.queryPlanExecutor = queryPlanExecutor;
    AnalyticsExecutorHolder.schemaProvider = schemaProvider;
  }

  /**
   * Returns the analytics-engine query plan executor as {@link Object}. Returns {@code null} when
   * analytics-engine is not installed (SPI never fired). Callers cast to {@code
   * QueryPlanExecutor<RelNode, Iterable<Object[]>>} only inside code paths that are gated on a
   * non-null return value — see {@code RestUnifiedQueryAction#fromUnknownExecutor}.
   */
  public static Object getQueryPlanExecutor() {
    return queryPlanExecutor;
  }

  /**
   * Returns the analytics-engine schema provider as {@link Object}. Returns {@code null} when
   * analytics-engine is not installed. Callers cast to {@code SchemaProvider} only inside code
   * paths that are gated on a non-null return value.
   */
  public static Object getSchemaProvider() {
    return schemaProvider;
  }
}

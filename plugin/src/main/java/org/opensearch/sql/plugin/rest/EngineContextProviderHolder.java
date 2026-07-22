/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import org.opensearch.analytics.EngineContextProvider;

/**
 * Bridge for sharing the analytics-engine {@link EngineContextProvider} singleton between the PPL
 * transport action (where Guice resolves the binding via {@code @Inject}) and the REST-only SQL
 * router (where Guice cannot, because {@code SQLPlugin#getRestHandlers} runs before the Node-level
 * injector satisfies {@code @Inject} parameters).
 *
 * <p>Mirrors {@link AnalyticsExecutorHolder} — same cross-plugin Guice gap, same solution. The
 * holder is populated once by {@link org.opensearch.sql.plugin.transport.TransportPPLQueryAction}'s
 * ctor and then read by the SQL router. The context itself is intended to be a stable singleton
 * produced by AnalyticsPlugin#createComponents.
 */
public final class EngineContextProviderHolder {

  private static volatile EngineContextProvider context;

  private EngineContextProviderHolder() {}

  public static void set(EngineContextProvider instance) {
    context = instance;
  }

  public static EngineContextProvider get() {
    return context;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

/**
 * Bridge for sharing the merged {@link RestEndpointRegistry} between plugin bootstrap (where the
 * SQL plugin builds it from the built-in provider plus every provider discovered via {@code
 * ExtensiblePlugin.loadExtensions}) and {@code OpenSearchStorageEngine.getTable} (which resolves a
 * {@code rest} endpoint at query time).
 *
 * <p>Why a static holder: {@code loadExtensions} runs during node bootstrap, before the Node-level
 * Guice injector exists, so the merged registry cannot be injected into the storage engine.
 * Publishing it here once at bootstrap lets the storage engine read the same instance without going
 * through the injector. Mirrors {@code AnalyticsExecutorHolder}.
 */
public final class RestEndpointRegistryHolder {

  private static volatile RestEndpointRegistry registry;

  private RestEndpointRegistryHolder() {}

  public static void set(RestEndpointRegistry instance) {
    registry = instance;
  }

  public static RestEndpointRegistry get() {
    return registry;
  }
}

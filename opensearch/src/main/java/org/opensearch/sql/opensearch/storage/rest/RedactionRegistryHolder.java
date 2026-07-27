/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

/**
 * Bridge for sharing the platform {@link RedactionRegistry} between plugin bootstrap (where the SQL
 * plugin publishes it) and {@code OpenSearchStorageEngine.getTable} (which applies it at query
 * time). Mirrors {@link RestEndpointRegistryHolder}.
 *
 * <p>Defaults to an empty registry so the OSS choke point is a pure no-op even before bootstrap
 * publishes one, and so {@code getTable} never has to null-check. A managed distribution replaces
 * or fills it via a wrapper patch that calls {@link #set} (or {@link RedactionRegistry#register})
 * during {@code createComponents}.
 */
public final class RedactionRegistryHolder {

  private static volatile RedactionRegistry registry = new RedactionRegistry();

  private RedactionRegistryHolder() {}

  public static void set(RedactionRegistry instance) {
    registry = instance == null ? new RedactionRegistry() : instance;
  }

  public static RedactionRegistry get() {
    return registry;
  }
}

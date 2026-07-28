/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import org.opensearch.sql.spi.rest.Redactor;

/**
 * Bridge for sharing the platform {@link Redactor} between plugin bootstrap (where the SQL plugin
 * publishes it) and {@code OpenSearchStorageEngine.getTable} (which applies it at query time).
 * Mirrors {@link RestEndpointRegistryHolder}.
 *
 * <p>Defaults to {@link Redactor#NONE} so the OSS choke point is a pure no-op even before bootstrap
 * runs and so {@code getTable} never has to null-check; a provider calls {@link #set} during
 * {@code createComponents} to install its own implementation. This is intentionally a single
 * central hook, not a {@code loadExtensions} SPI, so redaction has one owner rather than an open
 * registration surface.
 */
public final class RedactorHolder {

  private static volatile Redactor redactor = Redactor.NONE;

  private RedactorHolder() {}

  public static void set(Redactor instance) {
    redactor = instance == null ? Redactor.NONE : instance;
  }

  public static Redactor get() {
    return redactor;
  }
}

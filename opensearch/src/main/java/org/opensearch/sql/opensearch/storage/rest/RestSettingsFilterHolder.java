/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import org.opensearch.common.settings.SettingsFilter;

/**
 * Bridge for sharing the node-level {@link SettingsFilter} with the in-cluster {@code rest '
 * /_cluster/settings'} fetcher.
 *
 * <p>The native {@code GET /_cluster/settings} REST endpoint redacts settings registered with
 * {@code Property.Filtered} (or matched by a plugin-registered filter pattern) by running the
 * response through {@link SettingsFilter}. The PPL {@code rest} command's in-cluster path reads
 * {@code persistentSettings()}/{@code transientSettings()} straight from cluster state via the
 * transport layer, where no {@link SettingsFilter} is applied. To keep the command's redaction
 * behavior identical to the native endpoint, the node's {@link SettingsFilter} is published here.
 *
 * <p>Why a static holder: the {@link SettingsFilter} instance is only handed to the plugin in
 * {@code SQLPlugin#getRestHandlers}, which runs outside any Guice-managed lifecycle, while {@link
 * OpenSearchNodeClient} is built through the Node injector. Persisting the filter here once {@code
 * getRestHandlers} fires lets the fetcher read the same instance without going back through the
 * injector. This mirrors the existing {@code AnalyticsExecutorHolder} pattern.
 */
public final class RestSettingsFilterHolder {

  private static volatile SettingsFilter settingsFilter;

  private RestSettingsFilterHolder() {}

  public static void set(SettingsFilter instance) {
    settingsFilter = instance;
  }

  public static SettingsFilter get() {
    return settingsFilter;
  }
}

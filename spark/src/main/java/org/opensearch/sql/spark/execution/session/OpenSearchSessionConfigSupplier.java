/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;

@RequiredArgsConstructor
public class OpenSearchSessionConfigSupplier implements SessionConfigSupplier {
  private final Settings settings;

  @Override
  public Long getSessionInactivityTimeoutMillis() {
    return settings.getSettingValue(Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS);
  }
}

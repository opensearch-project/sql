/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.common.setting.Settings;

/**
 * Holder class to provide static access to Settings for UDAF functions. Since Calcite UDAF
 * functions are instantiated via reflection with default constructor, we need this static holder to
 * access settings.
 */
public class SettingsHolder {
  private static volatile Settings settings;

  /** Private constructor to prevent instantiation */
  private SettingsHolder() {}

  /**
   * Set the settings instance. This should be called during plugin initialization.
   *
   * @param s Settings instance
   */
  public static void setSettings(Settings s) {
    settings = s;
  }

  /**
   * Get the settings instance.
   *
   * @return Settings instance or null if not initialized
   */
  public static Settings getSettings() {
    return settings;
  }

  /**
   * Get the maximum limit for VALUES aggregate function.
   *
   * @return Maximum limit (0 means unlimited)
   */
  public static int getValuesMaxLimit() {
    if (settings != null) {
      Integer limit = settings.getSettingValue(Settings.Key.PPL_VALUES_MAX_LIMIT);
      return limit != null ? limit : 0;
    }
    return 0; // Default when settings not available
  }
}

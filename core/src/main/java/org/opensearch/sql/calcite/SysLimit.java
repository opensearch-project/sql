/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.opensearch.sql.common.setting.Settings;

public record SysLimit(Integer querySizeLimit, Integer subsearchLimit, Integer joinSubsearchLimit) {
  /** Create SysLimit from Settings. */
  public static SysLimit fromSettings(Settings settings) {
    return settings == null
        ? null
        : new SysLimit(
            settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT),
            settings.getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT),
            settings.getSettingValue(Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT));
  }

  /** No limitation on subsearch */
  public static SysLimit UNLIMITED_SUBSEARCH = new SysLimit(10000, -1, -1);

  /** For testing only */
  public static SysLimit DEFAULT = new SysLimit(10000, 10000, 50000);
}

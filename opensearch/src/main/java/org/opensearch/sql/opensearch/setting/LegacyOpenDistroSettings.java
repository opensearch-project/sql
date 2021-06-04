/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.sql.opensearch.setting;

import static org.opensearch.common.unit.TimeValue.timeValueMinutes;

import lombok.experimental.UtilityClass;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.LegacySettings;

@UtilityClass
public class LegacyOpenDistroSettings {

  public static final Setting<Boolean> SQL_ENABLED_SETTING = Setting.boolSetting(
      LegacySettings.Key.SQL_ENABLED.getKeyValue(),
      true,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<Integer> SQL_QUERY_SLOWLOG_SETTING = Setting.intSetting(
      LegacySettings.Key.SQL_QUERY_SLOWLOG.getKeyValue(),
      2,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<TimeValue> SQL_CURSOR_KEEPALIVE_SETTING = Setting.positiveTimeSetting(
      LegacySettings.Key.SQL_CURSOR_KEEPALIVE.getKeyValue(),
      timeValueMinutes(1),
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<Long> METRICS_ROLLING_WINDOW_SETTING = Setting.longSetting(
      LegacySettings.Key.METRICS_ROLLING_WINDOW.getKeyValue(),
      3600L,
      2L,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<Long> METRICS_ROLLING_INTERVAL_SETTING = Setting.longSetting(
      LegacySettings.Key.METRICS_ROLLING_INTERVAL.getKeyValue(),
      60L,
      1L,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<Boolean> PPL_ENABLED_SETTING = Setting.boolSetting(
      LegacySettings.Key.PPL_ENABLED.getKeyValue(),
      true,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<ByteSizeValue>
      PPL_QUERY_MEMORY_LIMIT_SETTING = Setting.memorySizeSetting(
      LegacySettings.Key.PPL_QUERY_MEMORY_LIMIT.getKeyValue(),
      "85%",
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  public static final Setting<Integer> QUERY_SIZE_LIMIT_SETTING = Setting.intSetting(
      LegacySettings.Key.QUERY_SIZE_LIMIT.getKeyValue(),
      200,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

}

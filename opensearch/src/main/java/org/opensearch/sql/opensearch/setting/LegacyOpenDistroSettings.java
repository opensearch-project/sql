/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.setting;

import static org.opensearch.common.unit.TimeValue.timeValueMinutes;

import com.google.common.collect.ImmutableList;
import java.util.List;
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

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the new engine is always enabled.
   */
  public static final Setting<Boolean> SQL_NEW_ENGINE_ENABLED_SETTING = Setting.boolSetting(
      LegacySettings.Key.SQL_NEW_ENGINE_ENABLED.getKeyValue(),
      true,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the query analysis in legacy engine is disabled.
   */
  public static final Setting<Boolean> QUERY_ANALYSIS_ENABLED_SETTING = Setting.boolSetting(
      LegacySettings.Key.QUERY_ANALYSIS_ENABLED.getKeyValue(),
      false,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the query analysis suggestion in legacy engine is disabled.
   */
  public static final Setting<Boolean> QUERY_ANALYSIS_SEMANTIC_SUGGESTION_SETTING =
      Setting.boolSetting(
      LegacySettings.Key.QUERY_ANALYSIS_SEMANTIC_SUGGESTION.getKeyValue(),
      false,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic,
      Setting.Property.Deprecated);

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the query analysis threshold in legacy engine is disabled.
   */
  public static final Setting<Integer> QUERY_ANALYSIS_SEMANTIC_THRESHOLD_SETTING =
      Setting.intSetting(
          LegacySettings.Key.QUERY_ANALYSIS_SEMANTIC_THRESHOLD.getKeyValue(),
          200,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic,
          Setting.Property.Deprecated);

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the query response format is default to JDBC format.
   */
  public static final Setting<String> QUERY_RESPONSE_FORMAT_SETTING =
      Setting.simpleString(
          LegacySettings.Key.QUERY_RESPONSE_FORMAT.getKeyValue(),
          "jdbc",
          Setting.Property.NodeScope,
          Setting.Property.Dynamic,
          Setting.Property.Deprecated);

  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the cursor feature is enabled by default.
   */
  public static final Setting<Boolean> SQL_CURSOR_ENABLED_SETTING =
      Setting.boolSetting(
          LegacySettings.Key.SQL_CURSOR_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic,
          Setting.Property.Deprecated);
  /**
   * Deprecated and will be removed then.
   * From OpenSearch 1.0, the fetch_size in query body will decide whether create the cursor
   * context. No cursor will be created if the fetch_size = 0.
   */
  public static final Setting<Integer> SQL_CURSOR_FETCH_SIZE_SETTING =
      Setting.intSetting(
          LegacySettings.Key.SQL_CURSOR_FETCH_SIZE.getKeyValue(),
          1000,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic,
          Setting.Property.Deprecated);

  /**
   * Used by Plugin to init Setting.
   */
  public static List<Setting<?>> legacySettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .add(SQL_ENABLED_SETTING)
        .add(SQL_QUERY_SLOWLOG_SETTING)
        .add(SQL_CURSOR_KEEPALIVE_SETTING)
        .add(METRICS_ROLLING_WINDOW_SETTING)
        .add(METRICS_ROLLING_INTERVAL_SETTING)
        .add(PPL_ENABLED_SETTING)
        .add(PPL_QUERY_MEMORY_LIMIT_SETTING)
        .add(QUERY_SIZE_LIMIT_SETTING)
        .add(SQL_NEW_ENGINE_ENABLED_SETTING)
        .add(QUERY_ANALYSIS_ENABLED_SETTING)
        .add(QUERY_ANALYSIS_SEMANTIC_SUGGESTION_SETTING)
        .add(QUERY_ANALYSIS_SEMANTIC_THRESHOLD_SETTING)
        .add(QUERY_RESPONSE_FORMAT_SETTING)
        .add(SQL_CURSOR_ENABLED_SETTING)
        .add(SQL_CURSOR_FETCH_SIZE_SETTING)
        .build();
  }
}

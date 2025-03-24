/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.setting;

import static org.opensearch.common.settings.Settings.EMPTY;
import static org.opensearch.common.unit.TimeValue.timeValueDays;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.sql.common.setting.Settings.Key.ENCYRPTION_MASTER_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.MemorySizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.common.setting.LegacySettings;
import org.opensearch.sql.common.setting.Settings;

/** Setting implementation on OpenSearch. */
@Log4j2
public class OpenSearchSettings extends Settings {
  /** Default settings. */
  private final Map<Settings.Key, Setting<?>> defaultSettings;

  /** Latest setting value for each registered key. Thread-safe is required. */
  @VisibleForTesting
  private final Map<Settings.Key, Object> latestSettings = new ConcurrentHashMap<>();

  public static final Setting<?> SQL_ENABLED_SETTING =
      Setting.boolSetting(
          Key.SQL_ENABLED.getKeyValue(),
          LegacyOpenDistroSettings.SQL_ENABLED_SETTING,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SQL_SLOWLOG_SETTING =
      Setting.intSetting(
          Key.SQL_SLOWLOG.getKeyValue(),
          LegacyOpenDistroSettings.SQL_QUERY_SLOWLOG_SETTING,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SQL_CURSOR_KEEP_ALIVE_SETTING =
      Setting.positiveTimeSetting(
          Key.SQL_CURSOR_KEEP_ALIVE.getKeyValue(),
          LegacyOpenDistroSettings.SQL_CURSOR_KEEPALIVE_SETTING,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SQL_DELETE_ENABLED_SETTING =
      Setting.boolSetting(
          Key.SQL_DELETE_ENABLED.getKeyValue(),
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SQL_PAGINATION_API_SEARCH_AFTER_SETTING =
      Setting.boolSetting(
          Key.SQL_PAGINATION_API_SEARCH_AFTER.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> PPL_ENABLED_SETTING =
      Setting.boolSetting(
          Key.PPL_ENABLED.getKeyValue(),
          LegacyOpenDistroSettings.PPL_ENABLED_SETTING,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> CALCITE_ENGINE_ENABLED_SETTING =
      Setting.boolSetting(
          Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> CALCITE_FALLBACK_ALLOWED_SETTING =
      Setting.boolSetting(
          Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> CALCITE_PUSHDOWN_ENABLED_SETTING =
      Setting.boolSetting(
          Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> QUERY_MEMORY_LIMIT_SETTING =
      new Setting<>(
          Key.QUERY_MEMORY_LIMIT.getKeyValue(),
          LegacyOpenDistroSettings.PPL_QUERY_MEMORY_LIMIT_SETTING,
          (s) ->
              MemorySizeValue.parseBytesSizeValueOrHeapRatio(
                  s, LegacySettings.Key.PPL_QUERY_MEMORY_LIMIT.getKeyValue()),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> QUERY_SIZE_LIMIT_SETTING =
      Setting.intSetting(
          Key.QUERY_SIZE_LIMIT.getKeyValue(),
          IndexSettings.MAX_RESULT_WINDOW_SETTING,
          0,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> METRICS_ROLLING_WINDOW_SETTING =
      Setting.longSetting(
          Key.METRICS_ROLLING_WINDOW.getKeyValue(),
          LegacyOpenDistroSettings.METRICS_ROLLING_WINDOW_SETTING,
          2L,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> METRICS_ROLLING_INTERVAL_SETTING =
      Setting.longSetting(
          Key.METRICS_ROLLING_INTERVAL.getKeyValue(),
          LegacyOpenDistroSettings.METRICS_ROLLING_INTERVAL_SETTING,
          1L,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // we are keeping this to not break upgrades if the config is already present.
  // This will be completely removed in 3.0.
  public static final Setting<InputStream> DATASOURCE_CONFIG =
      SecureSetting.secureFile(
          "plugins.query.federation.datasources.config", null, Setting.Property.Deprecated);

  public static final Setting<String> DATASOURCE_MASTER_SECRET_KEY =
      Setting.simpleString(
          ENCYRPTION_MASTER_KEY.getKeyValue(),
          Setting.Property.NodeScope,
          Setting.Property.Final,
          Setting.Property.Filtered);

  public static final Setting<List<String>> DATASOURCE_URI_HOSTS_DENY_LIST =
      Setting.listSetting(
          Key.DATASOURCES_URI_HOSTS_DENY_LIST.getKeyValue(),
          Collections.emptyList(),
          Function.identity(),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Boolean> DATASOURCE_ENABLED_SETTING =
      Setting.boolSetting(
          Key.DATASOURCES_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Boolean> ASYNC_QUERY_ENABLED_SETTING =
      Setting.boolSetting(
          Key.ASYNC_QUERY_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Boolean> ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED_SETTING =
      Setting.boolSetting(
          Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<String> ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL_SETTING =
      Setting.simpleString(
          Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL.getKeyValue(),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<String> SPARK_EXECUTION_ENGINE_CONFIG =
      Setting.simpleString(
          Key.SPARK_EXECUTION_ENGINE_CONFIG.getKeyValue(),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SPARK_EXECUTION_SESSION_LIMIT_SETTING =
      Setting.intSetting(
          Key.SPARK_EXECUTION_SESSION_LIMIT.getKeyValue(),
          10,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING =
      Setting.intSetting(
          Key.SPARK_EXECUTION_REFRESH_JOB_LIMIT.getKeyValue(),
          5,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<TimeValue> SESSION_INDEX_TTL_SETTING =
      Setting.positiveTimeSetting(
          Key.SESSION_INDEX_TTL.getKeyValue(),
          timeValueDays(30),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<TimeValue> RESULT_INDEX_TTL_SETTING =
      Setting.positiveTimeSetting(
          Key.RESULT_INDEX_TTL.getKeyValue(),
          timeValueDays(60),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Boolean> AUTO_INDEX_MANAGEMENT_ENABLED_SETTING =
      Setting.boolSetting(
          Key.AUTO_INDEX_MANAGEMENT_ENABLED.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> DATASOURCES_LIMIT_SETTING =
      Setting.intSetting(
          Key.DATASOURCES_LIMIT.getKeyValue(),
          20,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Long> SESSION_INACTIVITY_TIMEOUT_MILLIS_SETTING =
      Setting.longSetting(
          Key.SESSION_INACTIVITY_TIMEOUT_MILLIS.getKeyValue(),
          180000L,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<TimeValue> STREAMING_JOB_HOUSEKEEPER_INTERVAL_SETTING =
      Setting.positiveTimeSetting(
          Key.STREAMING_JOB_HOUSEKEEPER_INTERVAL.getKeyValue(),
          timeValueMinutes(15),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<?> FIELD_TYPE_TOLERANCE_SETTING =
      Setting.boolSetting(
          Key.FIELD_TYPE_TOLERANCE.getKeyValue(),
          true,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Construct OpenSearchSetting. The OpenSearchSetting must be singleton. */
  @SuppressWarnings("unchecked")
  public OpenSearchSettings(ClusterSettings clusterSettings) {
    ImmutableMap.Builder<Key, Setting<?>> settingBuilder = new ImmutableMap.Builder<>();
    register(
        settingBuilder,
        clusterSettings,
        Key.SQL_ENABLED,
        SQL_ENABLED_SETTING,
        new Updater(Key.SQL_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.SQL_SLOWLOG,
        SQL_SLOWLOG_SETTING,
        new Updater(Key.SQL_SLOWLOG));
    register(
        settingBuilder,
        clusterSettings,
        Key.SQL_CURSOR_KEEP_ALIVE,
        SQL_CURSOR_KEEP_ALIVE_SETTING,
        new Updater(Key.SQL_CURSOR_KEEP_ALIVE));
    register(
        settingBuilder,
        clusterSettings,
        Key.SQL_DELETE_ENABLED,
        SQL_DELETE_ENABLED_SETTING,
        new Updater(Key.SQL_DELETE_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.SQL_PAGINATION_API_SEARCH_AFTER,
        SQL_PAGINATION_API_SEARCH_AFTER_SETTING,
        new Updater(Key.SQL_PAGINATION_API_SEARCH_AFTER));
    register(
        settingBuilder,
        clusterSettings,
        Key.PPL_ENABLED,
        PPL_ENABLED_SETTING,
        new Updater(Key.PPL_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.CALCITE_ENGINE_ENABLED,
        CALCITE_ENGINE_ENABLED_SETTING,
        new Updater(Key.CALCITE_ENGINE_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.CALCITE_FALLBACK_ALLOWED,
        CALCITE_FALLBACK_ALLOWED_SETTING,
        new Updater(Key.CALCITE_FALLBACK_ALLOWED));
    register(
        settingBuilder,
        clusterSettings,
        Key.CALCITE_PUSHDOWN_ENABLED,
        CALCITE_PUSHDOWN_ENABLED_SETTING,
        new Updater(Key.CALCITE_PUSHDOWN_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.QUERY_MEMORY_LIMIT,
        QUERY_MEMORY_LIMIT_SETTING,
        new Updater(Key.QUERY_MEMORY_LIMIT));
    register(
        settingBuilder,
        clusterSettings,
        Key.QUERY_SIZE_LIMIT,
        QUERY_SIZE_LIMIT_SETTING,
        new Updater(Key.QUERY_SIZE_LIMIT));
    register(
        settingBuilder,
        clusterSettings,
        Key.METRICS_ROLLING_WINDOW,
        METRICS_ROLLING_WINDOW_SETTING,
        new Updater(Key.METRICS_ROLLING_WINDOW));
    register(
        settingBuilder,
        clusterSettings,
        Key.METRICS_ROLLING_INTERVAL,
        METRICS_ROLLING_INTERVAL_SETTING,
        new Updater(Key.METRICS_ROLLING_INTERVAL));
    register(
        settingBuilder,
        clusterSettings,
        Key.DATASOURCES_URI_HOSTS_DENY_LIST,
        DATASOURCE_URI_HOSTS_DENY_LIST,
        new Updater(Key.DATASOURCES_URI_HOSTS_DENY_LIST));
    register(
        settingBuilder,
        clusterSettings,
        Key.DATASOURCES_ENABLED,
        DATASOURCE_ENABLED_SETTING,
        new Updater(Key.DATASOURCES_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.ASYNC_QUERY_ENABLED,
        ASYNC_QUERY_ENABLED_SETTING,
        new Updater(Key.ASYNC_QUERY_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED,
        ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED_SETTING,
        new Updater(Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL,
        ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL_SETTING,
        new Updater(Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL));
    register(
        settingBuilder,
        clusterSettings,
        Key.SPARK_EXECUTION_ENGINE_CONFIG,
        SPARK_EXECUTION_ENGINE_CONFIG,
        new Updater(Key.SPARK_EXECUTION_ENGINE_CONFIG));
    register(
        settingBuilder,
        clusterSettings,
        Key.SPARK_EXECUTION_SESSION_LIMIT,
        SPARK_EXECUTION_SESSION_LIMIT_SETTING,
        new Updater(Key.SPARK_EXECUTION_SESSION_LIMIT));
    register(
        settingBuilder,
        clusterSettings,
        Key.SPARK_EXECUTION_REFRESH_JOB_LIMIT,
        SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING,
        new Updater(Key.SPARK_EXECUTION_REFRESH_JOB_LIMIT));
    register(
        settingBuilder,
        clusterSettings,
        Key.SESSION_INDEX_TTL,
        SESSION_INDEX_TTL_SETTING,
        new Updater(Key.SESSION_INDEX_TTL));
    register(
        settingBuilder,
        clusterSettings,
        Key.RESULT_INDEX_TTL,
        RESULT_INDEX_TTL_SETTING,
        new Updater(Key.RESULT_INDEX_TTL));
    register(
        settingBuilder,
        clusterSettings,
        Key.AUTO_INDEX_MANAGEMENT_ENABLED,
        AUTO_INDEX_MANAGEMENT_ENABLED_SETTING,
        new Updater(Key.AUTO_INDEX_MANAGEMENT_ENABLED));
    register(
        settingBuilder,
        clusterSettings,
        Key.DATASOURCES_LIMIT,
        DATASOURCES_LIMIT_SETTING,
        new Updater(Key.DATASOURCES_LIMIT));
    registerNonDynamicSettings(
        settingBuilder, clusterSettings, Key.CLUSTER_NAME, ClusterName.CLUSTER_NAME_SETTING);
    register(
        settingBuilder,
        clusterSettings,
        Key.SESSION_INACTIVITY_TIMEOUT_MILLIS,
        SESSION_INACTIVITY_TIMEOUT_MILLIS_SETTING,
        new Updater(Key.SESSION_INACTIVITY_TIMEOUT_MILLIS));
    register(
        settingBuilder,
        clusterSettings,
        Key.STREAMING_JOB_HOUSEKEEPER_INTERVAL,
        STREAMING_JOB_HOUSEKEEPER_INTERVAL_SETTING,
        new Updater(Key.STREAMING_JOB_HOUSEKEEPER_INTERVAL));
    register(
        settingBuilder,
        clusterSettings,
        Key.FIELD_TYPE_TOLERANCE,
        FIELD_TYPE_TOLERANCE_SETTING,
        new Updater(Key.FIELD_TYPE_TOLERANCE));
    defaultSettings = settingBuilder.build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSettingValue(Settings.Key key) {
    return (T) latestSettings.getOrDefault(key, defaultSettings.get(key).getDefault(EMPTY));
  }

  /** Register the pair of {key, setting}. */
  private void register(
      ImmutableMap.Builder<Key, Setting<?>> settingBuilder,
      ClusterSettings clusterSettings,
      Settings.Key key,
      Setting setting,
      Consumer<Object> updater) {
    if (clusterSettings.get(setting) != null) {
      latestSettings.put(key, clusterSettings.get(setting));
    }
    settingBuilder.put(key, setting);
    clusterSettings.addSettingsUpdateConsumer(setting, updater);
  }

  /** Register Non Dynamic Settings without consumer. */
  private void registerNonDynamicSettings(
      ImmutableMap.Builder<Key, Setting<?>> settingBuilder,
      ClusterSettings clusterSettings,
      Settings.Key key,
      Setting setting) {
    settingBuilder.put(key, setting);
    latestSettings.put(key, clusterSettings.get(setting));
  }

  /**
   * Add the inner class only for UT coverage purpose. Lambda could be much elegant solution. But
   * which is hard to test.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  class Updater implements Consumer {
    private final Settings.Key key;

    @Override
    public void accept(Object newValue) {
      log.debug("The value of setting [{}] changed to [{}]", key, newValue);
      latestSettings.put(key, newValue);
    }
  }

  /** Used by Plugin to init Setting. */
  public static List<Setting<?>> pluginSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .add(SQL_ENABLED_SETTING)
        .add(SQL_SLOWLOG_SETTING)
        .add(SQL_CURSOR_KEEP_ALIVE_SETTING)
        .add(SQL_DELETE_ENABLED_SETTING)
        .add(SQL_PAGINATION_API_SEARCH_AFTER_SETTING)
        .add(PPL_ENABLED_SETTING)
        .add(CALCITE_ENGINE_ENABLED_SETTING)
        .add(CALCITE_FALLBACK_ALLOWED_SETTING)
        .add(CALCITE_PUSHDOWN_ENABLED_SETTING)
        .add(QUERY_MEMORY_LIMIT_SETTING)
        .add(QUERY_SIZE_LIMIT_SETTING)
        .add(METRICS_ROLLING_WINDOW_SETTING)
        .add(METRICS_ROLLING_INTERVAL_SETTING)
        .add(DATASOURCE_URI_HOSTS_DENY_LIST)
        .add(DATASOURCE_ENABLED_SETTING)
        .add(ASYNC_QUERY_ENABLED_SETTING)
        .add(ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED_SETTING)
        .add(ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL_SETTING)
        .add(SPARK_EXECUTION_ENGINE_CONFIG)
        .add(SPARK_EXECUTION_SESSION_LIMIT_SETTING)
        .add(SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING)
        .add(SESSION_INDEX_TTL_SETTING)
        .add(RESULT_INDEX_TTL_SETTING)
        .add(AUTO_INDEX_MANAGEMENT_ENABLED_SETTING)
        .add(DATASOURCES_LIMIT_SETTING)
        .add(SESSION_INACTIVITY_TIMEOUT_MILLIS_SETTING)
        .add(STREAMING_JOB_HOUSEKEEPER_INTERVAL_SETTING)
        .add(FIELD_TYPE_TOLERANCE_SETTING)
        .build();
  }

  /** Init Non Dynamic Plugin Settings. */
  public static List<Setting<?>> pluginNonDynamicSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .add(DATASOURCE_MASTER_SECRET_KEY)
        .add(DATASOURCE_CONFIG)
        .build();
  }

  /** Used by local cluster to get settings from a setting instance. */
  public List<Setting<?>> getSettings() {
    return pluginSettings();
  }
}

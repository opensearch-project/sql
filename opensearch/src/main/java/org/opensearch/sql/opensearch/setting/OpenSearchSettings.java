/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.setting;

import static org.opensearch.common.settings.Settings.EMPTY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.MemorySizeValue;
import org.opensearch.sql.common.setting.LegacySettings;
import org.opensearch.sql.common.setting.Settings;

/**
 * Setting implementation on OpenSearch.
 */
@Log4j2
public class OpenSearchSettings extends Settings {
  /**
   * Default settings.
   */
  private final Map<Settings.Key, Setting<?>> defaultSettings;
  /**
   * Latest setting value for each registered key. Thread-safe is required.
   */
  @VisibleForTesting
  private final Map<Settings.Key, Object> latestSettings = new ConcurrentHashMap<>();

  public static final Setting<?> SQL_ENABLED_SETTING = Setting.boolSetting(
      Key.SQL_ENABLED.getKeyValue(),
      LegacyOpenDistroSettings.SQL_ENABLED_SETTING,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> SQL_SLOWLOG_SETTING = Setting.intSetting(
      Key.SQL_SLOWLOG.getKeyValue(),
      LegacyOpenDistroSettings.SQL_QUERY_SLOWLOG_SETTING,
      0,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> SQL_CURSOR_KEEP_ALIVE_SETTING = Setting.positiveTimeSetting(
      Key.SQL_CURSOR_KEEP_ALIVE.getKeyValue(),
      LegacyOpenDistroSettings.SQL_CURSOR_KEEPALIVE_SETTING,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> SQL_DELETE_ENABLED_SETTING = Setting.boolSetting(
      Key.SQL_DELETE_ENABLED.getKeyValue(),
      false,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> PPL_ENABLED_SETTING = Setting.boolSetting(
      Key.PPL_ENABLED.getKeyValue(),
      LegacyOpenDistroSettings.PPL_ENABLED_SETTING,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> QUERY_MEMORY_LIMIT_SETTING = new Setting<>(
      Key.QUERY_MEMORY_LIMIT.getKeyValue(),
      LegacyOpenDistroSettings.PPL_QUERY_MEMORY_LIMIT_SETTING,
      (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(
          s, LegacySettings.Key.PPL_QUERY_MEMORY_LIMIT.getKeyValue()),
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> QUERY_SIZE_LIMIT_SETTING = Setting.intSetting(
      Key.QUERY_SIZE_LIMIT.getKeyValue(),
      LegacyOpenDistroSettings.QUERY_SIZE_LIMIT_SETTING,
      0,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> METRICS_ROLLING_WINDOW_SETTING = Setting.longSetting(
      Key.METRICS_ROLLING_WINDOW.getKeyValue(),
      LegacyOpenDistroSettings.METRICS_ROLLING_WINDOW_SETTING,
      2L,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<?> METRICS_ROLLING_INTERVAL_SETTING = Setting.longSetting(
      Key.METRICS_ROLLING_INTERVAL.getKeyValue(),
      LegacyOpenDistroSettings.METRICS_ROLLING_INTERVAL_SETTING,
      1L,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  /**
   * Construct ElasticsearchSetting.
   * The ElasticsearchSetting must be singleton.
   */
  @SuppressWarnings("unchecked")
  public OpenSearchSettings(ClusterSettings clusterSettings) {
    ImmutableMap.Builder<Key, Setting<?>> settingBuilder = new ImmutableMap.Builder<>();
    register(settingBuilder, clusterSettings, Key.SQL_ENABLED,
        SQL_ENABLED_SETTING, new Updater(Key.SQL_ENABLED));
    register(settingBuilder, clusterSettings, Key.SQL_SLOWLOG,
        SQL_SLOWLOG_SETTING, new Updater(Key.SQL_SLOWLOG));
    register(settingBuilder, clusterSettings, Key.SQL_CURSOR_KEEP_ALIVE,
        SQL_CURSOR_KEEP_ALIVE_SETTING, new Updater(Key.SQL_CURSOR_KEEP_ALIVE));
    register(settingBuilder, clusterSettings, Key.SQL_DELETE_ENABLED,
        SQL_DELETE_ENABLED_SETTING, new Updater(Key.SQL_DELETE_ENABLED));
    register(settingBuilder, clusterSettings, Key.PPL_ENABLED,
        PPL_ENABLED_SETTING, new Updater(Key.PPL_ENABLED));
    register(settingBuilder, clusterSettings, Key.QUERY_MEMORY_LIMIT,
        QUERY_MEMORY_LIMIT_SETTING, new Updater(Key.QUERY_MEMORY_LIMIT));
    register(settingBuilder, clusterSettings, Key.QUERY_SIZE_LIMIT,
        QUERY_SIZE_LIMIT_SETTING, new Updater(Key.QUERY_SIZE_LIMIT));
    register(settingBuilder, clusterSettings, Key.METRICS_ROLLING_WINDOW,
        METRICS_ROLLING_WINDOW_SETTING, new Updater(Key.METRICS_ROLLING_WINDOW));
    register(settingBuilder, clusterSettings, Key.METRICS_ROLLING_INTERVAL,
        METRICS_ROLLING_INTERVAL_SETTING, new Updater(Key.METRICS_ROLLING_INTERVAL));
    defaultSettings = settingBuilder.build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSettingValue(Settings.Key key) {
    return (T) latestSettings.getOrDefault(key, defaultSettings.get(key).getDefault(EMPTY));
  }

  /**
   * Register the pair of {key, setting}.
   */
  private void register(ImmutableMap.Builder<Key, Setting<?>> settingBuilder,
                        ClusterSettings clusterSettings, Settings.Key key,
                        Setting setting,
                        Consumer<Object> updater) {
    settingBuilder.put(key, setting);
    clusterSettings
        .addSettingsUpdateConsumer(setting, updater);
  }

  /**
   * Add the inner class only for UT coverage purpose.
   * Lambda could be much elegant solution. But which is hard to test.
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

  /**
   * Used by Plugin to init Setting.
   */
  public static List<Setting<?>> pluginSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .add(SQL_ENABLED_SETTING)
        .add(SQL_SLOWLOG_SETTING)
        .add(SQL_CURSOR_KEEP_ALIVE_SETTING)
        .add(SQL_DELETE_ENABLED_SETTING)
        .add(PPL_ENABLED_SETTING)
        .add(QUERY_MEMORY_LIMIT_SETTING)
        .add(QUERY_SIZE_LIMIT_SETTING)
        .add(METRICS_ROLLING_WINDOW_SETTING)
        .add(METRICS_ROLLING_INTERVAL_SETTING)
        .build();
  }

  /**
   * Used by local cluster to get settings from a setting instance.
   */
  public List<Setting<?>> getSettings() {
    return pluginSettings();
  }
}

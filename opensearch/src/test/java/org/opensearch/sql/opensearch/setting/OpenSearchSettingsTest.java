/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.setting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.sql.opensearch.setting.LegacyOpenDistroSettings.PPL_ENABLED_SETTING;
import static org.opensearch.sql.opensearch.setting.LegacyOpenDistroSettings.legacySettings;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.sql.common.setting.LegacySettings;
import org.opensearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class OpenSearchSettingsTest {

  @Mock
  private ClusterSettings clusterSettings;

  @Test
  void getSettingValue() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING)))))
        .thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue sizeValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotNull(sizeValue);
  }

  @Test
  void getSettingValueWithPresetValuesInYml() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings
        .get((Setting<ByteSizeValue>) OpenSearchSettings.QUERY_MEMORY_LIMIT_SETTING))
        .thenReturn(new ByteSizeValue(20));
    when(clusterSettings.get(not(or(eq(ClusterName.CLUSTER_NAME_SETTING),
        eq((Setting<ByteSizeValue>) OpenSearchSettings.QUERY_MEMORY_LIMIT_SETTING)))))
        .thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue sizeValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
    assertEquals(sizeValue, new ByteSizeValue(20));
  }

  @Test
  void pluginSettings() {
    List<Setting<?>> settings = OpenSearchSettings.pluginSettings();

    assertFalse(settings.isEmpty());
  }

  @Test
  void pluginNonDynamicSettings() {
    List<Setting<?>> settings = OpenSearchSettings.pluginNonDynamicSettings();

    assertFalse(settings.isEmpty());
  }

  @Test
  void getSettings() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING)))))
        .thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    assertFalse(settings.getSettings().isEmpty());
  }

  @Test
  void update() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING)))))
        .thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue oldValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
    OpenSearchSettings.Updater updater =
        settings.new Updater(Settings.Key.QUERY_MEMORY_LIMIT);
    updater.accept(new ByteSizeValue(0L));

    ByteSizeValue newValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotEquals(newValue.getBytes(), oldValue.getBytes());
  }

  @Test
  void settingsFallback() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING)))))
        .thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_ENABLED),
        LegacyOpenDistroSettings.SQL_ENABLED_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_SLOWLOG),
        LegacyOpenDistroSettings.SQL_QUERY_SLOWLOG_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE),
        LegacyOpenDistroSettings.SQL_CURSOR_KEEPALIVE_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.PPL_ENABLED),
        LegacyOpenDistroSettings.PPL_ENABLED_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT),
        LegacyOpenDistroSettings.PPL_QUERY_MEMORY_LIMIT_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT),
        LegacyOpenDistroSettings.QUERY_SIZE_LIMIT_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW),
        LegacyOpenDistroSettings.METRICS_ROLLING_WINDOW_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL),
        LegacyOpenDistroSettings.METRICS_ROLLING_INTERVAL_SETTING.get(
            org.opensearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void updateLegacySettingsFallback() {
    org.opensearch.common.settings.Settings settings =
        org.opensearch.common.settings.Settings.builder()
            .put(LegacySettings.Key.SQL_ENABLED.getKeyValue(), false)
            .put(LegacySettings.Key.SQL_QUERY_SLOWLOG.getKeyValue(), 10)
            .put(LegacySettings.Key.SQL_CURSOR_KEEPALIVE.getKeyValue(), timeValueMinutes(1))
            .put(LegacySettings.Key.PPL_ENABLED.getKeyValue(), true)
            .put(LegacySettings.Key.PPL_QUERY_MEMORY_LIMIT.getKeyValue(), "20%")
            .put(LegacySettings.Key.QUERY_SIZE_LIMIT.getKeyValue(), 100)
            .put(LegacySettings.Key.METRICS_ROLLING_WINDOW.getKeyValue(), 2000L)
            .put(LegacySettings.Key.METRICS_ROLLING_INTERVAL.getKeyValue(), 100L)
            .build();

    assertEquals(OpenSearchSettings.SQL_ENABLED_SETTING.get(settings), false);
    assertEquals(OpenSearchSettings.SQL_SLOWLOG_SETTING.get(settings), 10);
    assertEquals(OpenSearchSettings.SQL_CURSOR_KEEP_ALIVE_SETTING.get(settings),
        timeValueMinutes(1));
    assertEquals(OpenSearchSettings.PPL_ENABLED_SETTING.get(settings), true);
    assertEquals(OpenSearchSettings.QUERY_MEMORY_LIMIT_SETTING.get(settings),
        new ByteSizeValue((int) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2)));
    assertEquals(OpenSearchSettings.QUERY_SIZE_LIMIT_SETTING.get(settings), 100);
    assertEquals(OpenSearchSettings.METRICS_ROLLING_WINDOW_SETTING.get(settings), 2000L);
    assertEquals(OpenSearchSettings.METRICS_ROLLING_INTERVAL_SETTING.get(settings), 100L);
  }


  @Test
  void legacySettingsShouldBeDeprecatedBeforeRemove() {
    assertEquals(15, legacySettings().size());
  }
}

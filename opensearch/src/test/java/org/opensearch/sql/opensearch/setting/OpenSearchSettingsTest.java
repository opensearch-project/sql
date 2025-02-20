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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED_SETTING;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL_SETTING;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.QUERY_MEMORY_LIMIT_SETTING;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.SPARK_EXECUTION_ENGINE_CONFIG;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class OpenSearchSettingsTest {

  @Mock private ClusterSettings clusterSettings;

  @Test
  void getSettingValue() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING))))).thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue sizeValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotNull(sizeValue);
  }

  @Test
  void getSettingValueWithPresetValuesInYml() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get((Setting<ByteSizeValue>) QUERY_MEMORY_LIMIT_SETTING))
        .thenReturn(new ByteSizeValue(20));
    when(clusterSettings.get(
            not(
                or(
                    eq(ClusterName.CLUSTER_NAME_SETTING),
                    eq((Setting<ByteSizeValue>) QUERY_MEMORY_LIMIT_SETTING)))))
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
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING))))).thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    assertFalse(settings.getSettings().isEmpty());
  }

  @Test
  void update() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    when(clusterSettings.get(not((eq(ClusterName.CLUSTER_NAME_SETTING))))).thenReturn(null);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue oldValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
    OpenSearchSettings.Updater updater = settings.new Updater(Settings.Key.QUERY_MEMORY_LIMIT);
    updater.accept(new ByteSizeValue(0L));

    ByteSizeValue newValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotEquals(newValue.getBytes(), oldValue.getBytes());
  }

  @Test
  void getSparkExecutionEngineConfigSetting() {
    // Default is empty string
    assertEquals(
        "",
        SPARK_EXECUTION_ENGINE_CONFIG.get(
            org.opensearch.common.settings.Settings.builder().build()));

    // Configurable at runtime
    String sparkConfig =
        "{\n"
            + "  \"sparkSubmitParameters\": \"--conf spark.dynamicAllocation.enabled=false\"\n"
            + "}";
    assertEquals(
        sparkConfig,
        SPARK_EXECUTION_ENGINE_CONFIG.get(
            org.opensearch.common.settings.Settings.builder()
                .put(SPARK_EXECUTION_ENGINE_CONFIG.getKey(), sparkConfig)
                .build()));
  }

  @Test
  void getAsyncQueryExternalSchedulerEnabledSetting() {
    // Default is true
    assertEquals(
        true,
        ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED_SETTING.get(
            org.opensearch.common.settings.Settings.builder().build()));
  }

  @Test
  void getAsyncQueryExternalSchedulerIntervalSetting() {
    // Default is empty string
    assertEquals(
        "",
        ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL_SETTING.get(
            org.opensearch.common.settings.Settings.builder().build()));
  }
}

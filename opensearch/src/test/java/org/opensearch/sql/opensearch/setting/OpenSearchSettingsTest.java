/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.setting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.sql.opensearch.setting.LegacyOpenDistroSettings.legacySettings;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    ByteSizeValue sizeValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotNull(sizeValue);
  }

  @Test
  void pluginSettings() {
    List<Setting<?>> settings = OpenSearchSettings.pluginSettings();

    assertFalse(settings.isEmpty());
  }

  @Test
  void getSettings() {
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    assertFalse(settings.getSettings().isEmpty());
  }

  @Test
  void update() {
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

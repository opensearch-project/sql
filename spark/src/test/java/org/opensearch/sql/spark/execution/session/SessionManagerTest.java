/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;

@ExtendWith(MockitoExtension.class)
public class SessionManagerTest {
  @Mock private StateStore stateStore;
  @Mock private EMRServerlessClient emrClient;

  @Test
  public void sessionEnable() {
    Assertions.assertTrue(
        new SessionManager(stateStore, emrClient, sessionSetting(true)).isEnabled());
    Assertions.assertFalse(
        new SessionManager(stateStore, emrClient, sessionSetting(false)).isEnabled());
  }

  public static Settings sessionSetting(boolean enabled) {
    Map<Settings.Key, Object> settings = new HashMap<>();
    settings.put(Settings.Key.SPARK_EXECUTION_SESSION_ENABLED, enabled);
    return settings(settings);
  }

  public static Settings settings(Map<Settings.Key, Object> settings) {
    return new Settings() {
      @Override
      public <T> T getSettingValue(Key key) {
        return (T) settings.get(key);
      }

      @Override
      public List<?> getSettings() {
        return (List<?>) settings;
      }
    };
  }
}

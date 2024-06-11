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
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;

@ExtendWith(MockitoExtension.class)
public class SessionManagerTest {
  @Mock private SessionStorageService sessionStorageService;
  @Mock private StatementStorageService statementStorageService;
  @Mock private EMRServerlessClientFactory emrServerlessClientFactory;
  @Mock private SessionConfigSupplier sessionConfigSupplier;
  @Mock private SessionIdProvider sessionIdProvider;

  @Test
  public void sessionEnable() {
    SessionManager sessionManager =
        new SessionManager(
            sessionStorageService,
            statementStorageService,
            emrServerlessClientFactory,
            sessionConfigSupplier,
            sessionIdProvider);

    Assertions.assertTrue(sessionManager.isEnabled());
  }

  public static org.opensearch.sql.common.setting.Settings sessionSetting() {
    Map<org.opensearch.sql.common.setting.Settings.Key, Object> settings = new HashMap<>();
    settings.put(Settings.Key.SPARK_EXECUTION_SESSION_LIMIT, 100);
    settings.put(
        org.opensearch.sql.common.setting.Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS, 10000L);
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

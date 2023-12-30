/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.common.setting.Settings.Key.SESSION_INACTIVITY_TIMEOUT_MILLIS;
import static org.opensearch.sql.spark.execution.session.SessionId.newSessionId;

import java.util.Optional;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.utils.RealTimeProvider;

/**
 * Singleton Class
 *
 * <p>todo. add Session cache and Session sweeper.
 */
public class SessionManager {
  private final StateStore stateStore;
  private final EMRServerlessClient emrServerlessClient;
  private Settings settings;

  public SessionManager(
      StateStore stateStore, EMRServerlessClient emrServerlessClient, Settings settings) {
    this.stateStore = stateStore;
    this.emrServerlessClient = emrServerlessClient;
    this.settings = settings;
  }

  public Session createSession(CreateSessionRequest request) {
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(newSessionId(request.getDatasourceName()))
            .stateStore(stateStore)
            .serverlessClient(emrServerlessClient)
            .build();
    session.open(request);
    return session;
  }

  /**
   * Retrieves the session associated with the given session ID.
   *
   * <p>This method is particularly used in scenarios where the data source encoded in the session
   * ID is deemed untrustworthy. It allows for the safe retrieval of session details based on a
   * known and validated session ID, rather than relying on potentially outdated data source
   * information.
   *
   * <p>For more context on the use case and implementation, refer to the documentation here:
   * https://tinyurl.com/bdh6s834
   *
   * @param sid The unique identifier of the session. It is used to fetch the corresponding session
   *     details.
   * @param dataSourceName The name of the data source. This parameter is utilized in the session
   *     retrieval process.
   * @return An Optional containing the session associated with the provided session ID. Returns an
   *     empty Optional if no matching session is found.
   */
  public Optional<Session> getSession(SessionId sid, String dataSourceName) {
    Optional<SessionModel> model =
        StateStore.getSession(stateStore, dataSourceName).apply(sid.getSessionId());
    if (model.isPresent()) {
      InteractiveSession session =
          InteractiveSession.builder()
              .sessionId(sid)
              .stateStore(stateStore)
              .serverlessClient(emrServerlessClient)
              .sessionModel(model.get())
              .sessionInactivityTimeoutMilli(
                  settings.getSettingValue(SESSION_INACTIVITY_TIMEOUT_MILLIS))
              .timeProvider(new RealTimeProvider())
              .build();
      return Optional.ofNullable(session);
    }
    return Optional.empty();
  }

  /**
   * Retrieves the session associated with the provided session ID.
   *
   * <p>This method is utilized specifically in scenarios where the data source information encoded
   * in the session ID is considered trustworthy. It ensures the retrieval of session details based
   * on the session ID, relying on the integrity of the data source information contained within it.
   *
   * @param sid The session ID used to identify and retrieve the corresponding session. It is
   *     expected to contain valid and trusted data source information.
   * @return An Optional containing the session associated with the provided session ID. If no
   *     session is found that matches the session ID, an empty Optional is returned.
   */
  public Optional<Session> getSession(SessionId sid) {
    return getSession(sid, sid.getDataSourceName());
  }

  // todo, keep it only for testing, will remove it later.
  public boolean isEnabled() {
    return true;
  }
}

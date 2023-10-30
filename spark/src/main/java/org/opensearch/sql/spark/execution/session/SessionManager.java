/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionId.newSessionId;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;

/**
 * Singleton Class
 *
 * <p>todo. add Session cache and Session sweeper.
 */
@RequiredArgsConstructor
public class SessionManager {
  private final StateStore stateStore;
  private final EMRServerlessClient emrServerlessClient;
  private final Settings settings;

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

  public Optional<Session> getSession(SessionId sid) {
    Optional<SessionModel> model =
        StateStore.getSession(stateStore, sid.getDataSourceName()).apply(sid.getSessionId());
    if (model.isPresent()) {
      InteractiveSession session =
          InteractiveSession.builder()
              .sessionId(sid)
              .stateStore(stateStore)
              .serverlessClient(emrServerlessClient)
              .sessionModel(model.get())
              .build();
      return Optional.ofNullable(session);
    }
    return Optional.empty();
  }

  // todo, keep it only for testing, will remove it later.
  public boolean isEnabled() {
    return true;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionId.newSessionId;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;

/**
 * Singleton Class
 *
 * <p>todo. add Session cache and Session sweeper.
 */
@RequiredArgsConstructor
public class SessionManager {
  private final SessionStateStore stateStore;
  private final EMRServerlessClient emrServerlessClient;

  public Session createSession(CreateSessionRequest request) {
    InteractiveSession session =
        InteractiveSession.builder()
            .sessionId(newSessionId())
            .sessionStateStore(stateStore)
            .serverlessClient(emrServerlessClient)
            .build();
    session.open(request);
    return session;
  }

  public Optional<Session> getSession(SessionId sid) {
    Optional<SessionModel> model = stateStore.get(sid);
    if (model.isPresent()) {
      InteractiveSession session =
          InteractiveSession.builder()
              .sessionId(sid)
              .sessionStateStore(stateStore)
              .serverlessClient(emrServerlessClient)
              .sessionModel(model.get())
              .build();
      return Optional.ofNullable(session);
    }
    return Optional.empty();
  }
}

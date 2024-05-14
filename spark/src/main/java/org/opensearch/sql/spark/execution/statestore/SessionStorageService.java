/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;

/**
 * Interface for accessing {@link SessionModel} data storage.
 */
public interface SessionStorageService {

  SessionModel createSession(SessionModel sessionModel, String datasourceName);

  Optional<SessionModel> getSession(String id, String datasourceName);

  SessionModel updateSessionState(
      SessionModel sessionModel, SessionState sessionState, String datasourceName);
}

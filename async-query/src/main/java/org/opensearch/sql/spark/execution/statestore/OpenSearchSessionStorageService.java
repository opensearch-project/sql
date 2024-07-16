/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;

@RequiredArgsConstructor
public class OpenSearchSessionStorageService implements SessionStorageService {
  private static final Logger LOG = LogManager.getLogger();

  private final StateStore stateStore;
  private final SessionModelXContentSerializer serializer;

  @Override
  public SessionModel createSession(
      SessionModel sessionModel, AsyncQueryRequestContext asyncQueryRequestContext) {
    try {
      return stateStore.create(
          sessionModel.getId(),
          sessionModel,
          SessionModel::of,
          OpenSearchStateStoreUtil.getIndexName(sessionModel.getDatasourceName()));
    } catch (VersionConflictEngineException e) {
      String errorMsg = "session already exist. " + sessionModel.getSessionId();
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  @Override
  public Optional<SessionModel> getSession(String id, String datasourceName) {
    return stateStore.get(
        id, serializer::fromXContent, OpenSearchStateStoreUtil.getIndexName(datasourceName));
  }

  @Override
  public SessionModel updateSessionState(SessionModel sessionModel, SessionState sessionState) {
    return stateStore.updateState(
        sessionModel,
        sessionState,
        SessionModel::copyWithState,
        OpenSearchStateStoreUtil.getIndexName(sessionModel.getDatasourceName()));
  }
}

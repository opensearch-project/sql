/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;

@RequiredArgsConstructor
public class OpenSearchSessionStorageService implements SessionStorageService {

  private final StateStore stateStore;
  private final SessionModelXContentSerializer serializer;

  @Override
  public SessionModel createSession(SessionModel sessionModel) {
    return stateStore.create(
        sessionModel,
        SessionModel::of,
        OpenSearchStateStoreUtil.getIndexName(sessionModel.getDatasourceName()));
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

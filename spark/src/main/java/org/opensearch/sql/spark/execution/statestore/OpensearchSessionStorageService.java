package org.opensearch.sql.spark.execution.statestore;

import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;

@RequiredArgsConstructor
public class OpensearchSessionStorageService implements SessionStorageService {

  private final StateStore stateStore;

  @Override
  public SessionModel createSession(SessionModel sessionModel, String datasourceName) {
    return stateStore.create(
        sessionModel, SessionModel::of, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public Optional<SessionModel> getSession(String id, String datasourceName) {
    SessionModelXContentSerializer serializer = new SessionModelXContentSerializer();
    return stateStore.get(
        id, serializer::fromXContent, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }

  @Override
  public SessionModel updateSessionState(
      SessionModel sessionModel, SessionState sessionState, String datasourceName) {
    return stateStore.updateState(
        sessionModel,
        sessionState,
        SessionModel::copyWithState,
        DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
  }
}

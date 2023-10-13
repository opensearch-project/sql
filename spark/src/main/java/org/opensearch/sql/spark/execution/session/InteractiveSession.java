/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionModel.initInteractiveSession;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;

/**
 * Interactive session.
 *
 * <p>ENTRY_STATE: not_started
 */
@Getter
@Builder
public class InteractiveSession implements Session {
  private static final Logger LOG = LogManager.getLogger();

  private final SessionId sessionId;
  private final SessionStateStore sessionStateStore;
  private final EMRServerlessClient serverlessClient;

  private SessionModel sessionModel;

  @Override
  public void open(CreateSessionRequest createSessionRequest) {
    try {
      String jobID = serverlessClient.startJobRun(createSessionRequest.getStartJobRequest());
      String applicationId = createSessionRequest.getStartJobRequest().getApplicationId();

      sessionModel =
          initInteractiveSession(
              applicationId, jobID, sessionId, createSessionRequest.getDatasourceName());
      sessionStateStore.create(sessionModel);
    } catch (VersionConflictEngineException e) {
      String errorMsg = "session already exist. " + sessionId;
      LOG.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  @Override
  public void close() {
    Optional<SessionModel> model = sessionStateStore.get(sessionModel.getSessionId());
    if (model.isEmpty()) {
      throw new IllegalStateException("session not exist. " + sessionModel.getSessionId());
    } else {
      serverlessClient.cancelJobRun(sessionModel.getApplicationId(), sessionModel.getJobId());
    }
  }
}

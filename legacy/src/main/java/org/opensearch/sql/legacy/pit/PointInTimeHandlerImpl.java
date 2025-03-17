/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;

import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.transport.client.Client;

/** Handler for Point In Time */
public class PointInTimeHandlerImpl implements PointInTimeHandler {
  private final Client client;
  private String[] indices;
  @Getter @Setter private String pitId;
  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor for class
   *
   * @param client OpenSearch client
   * @param indices list of indices
   */
  public PointInTimeHandlerImpl(Client client, String[] indices) {
    this.client = client;
    this.indices = indices;
  }

  /**
   * Constructor for class
   *
   * @param client OpenSearch client
   * @param pitId Point In Time ID
   */
  public PointInTimeHandlerImpl(Client client, String pitId) {
    this.client = client;
    this.pitId = pitId;
  }

  /** Create PIT for given indices */
  @Override
  public void create() {
    CreatePitRequest createPitRequest =
        new CreatePitRequest(
            LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE), false, indices);
    ActionFuture<CreatePitResponse> execute =
        client.execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      pitId = pitResponse.getId();
      LOG.info("Created Point In Time {} successfully.", pitId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error occurred while creating PIT.", e);
    }
  }

  /** Delete PIT */
  @Override
  public void delete() {
    DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
    ActionFuture<DeletePitResponse> execute =
        client.execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      DeletePitResponse deletePitResponse = execute.get();
      LOG.info("Delete Point In Time {} status: {}", pitId, deletePitResponse.status().getStatus());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error occurred while deleting PIT.", e);
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

/** Handler for Point In Time */
public class PointInTimeHandlerImpl implements PointInTimeHandler {
  private Client client;
  private String[] indices;
  @Getter @Setter private String pitId;
  private Boolean deleteStatus = null;
  private Boolean createStatus = null;
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

  /**
   * Create PIT for given indices
   *
   * @return Point In Time creation status
   */
  @Override
  public boolean create() {
    CreatePitRequest createPitRequest =
        new CreatePitRequest(
            LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE), false, indices);
    client.createPit(
        createPitRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(CreatePitResponse createPitResponse) {
            pitId = createPitResponse.getId();
            createStatus = true;
            LOG.info("Created Point In Time {} successfully.", pitId);
          }

          @Override
          public void onFailure(Exception e) {
            createStatus = false;
            LOG.error("Error occurred while creating PIT", e);
          }
        });
    while (createStatus == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Error occurred while creating PIT", e);
      }
    }
    return createStatus;
  }

  /**
   * Delete PIT
   *
   * @return delete status
   */
  @Override
  public boolean delete() {
    DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
    client.deletePits(
        deletePitRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(DeletePitResponse deletePitResponse) {
            deleteStatus = true;
            LOG.info(
                "Delete Point In Time {} status: {}",
                pitId,
                deletePitResponse.status().getStatus());
          }

          @Override
          public void onFailure(Exception e) {
            deleteStatus = false;
            LOG.error("Error occurred while deleting PIT", e);
          }
        });
    while (deleteStatus == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Error occurred while deleting PIT", e);
      }
    }

    return deleteStatus;
  }
}

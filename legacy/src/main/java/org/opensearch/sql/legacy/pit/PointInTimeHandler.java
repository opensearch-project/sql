/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

public class PointInTimeHandler {
  private Client client;
  @Getter private String pitId;
  private static final Logger LOG = LogManager.getLogger();

  public PointInTimeHandler(Client client, String[] indices) {
    this.client = client;
    CreatePitRequest createPitRequest =
        new CreatePitRequest(
            LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE), false, indices);
    client.createPit(
        createPitRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(CreatePitResponse createPitResponse) {
            pitId = createPitResponse.getId();
          }

          @Override
          public void onFailure(Exception e) {
            LOG.error("Error occurred while creating PIT", e);
          }
        });
  }

  public void deletePointInTime() {
    DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
    client.deletePits(
        deletePitRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(org.opensearch.action.search.DeletePitResponse deletePitResponse) {
            LOG.debug(deletePitResponse);
          }

          @Override
          public void onFailure(Exception e) {
            LOG.error("Error occurred while deleting PIT", e);
          }
        });
  }
}

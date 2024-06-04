/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;

public class PointInTimeHandler {
  private Client client;
  private String pitId;
  private static final Logger LOG = LogManager.getLogger();

  public PointInTimeHandler(Client client, String[] indices) {
    this.client = client;

    CreatePitRequest createPitRequest = new CreatePitRequest(new TimeValue(600000), false, indices);
    client.createPit(
        createPitRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(CreatePitResponse createPitResponse) {
            pitId = createPitResponse.getId();
          }

          @Override
          public void onFailure(Exception e) {
            LOG.error("Error occurred while creating PIT" + e);
          }
        });
  }

  public String getPointInTimeId() {
    return pitId;
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
            LOG.error("Error occurred while deleting PIT" + e);
          }
        });
  }
}

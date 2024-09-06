/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;

@ExtendWith(MockitoExtension.class)
public class OpenSearchFlintIndexClientTest {

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client client;

  @Mock private AcknowledgedResponse acknowledgedResponse;

  @InjectMocks private OpenSearchFlintIndexClient openSearchFlintIndexClient;

  @Test
  public void testDeleteIndex() {
    when(client.admin().indices().delete(any(DeleteIndexRequest.class)).actionGet())
        .thenReturn(acknowledgedResponse);
    when(acknowledgedResponse.isAcknowledged()).thenReturn(true);

    openSearchFlintIndexClient.deleteIndex("test-index");
    verify(client.admin().indices()).delete(any(DeleteIndexRequest.class));
    verify(acknowledgedResponse).isAcknowledged();
  }
}

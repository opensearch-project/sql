/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchBulkWriterTest {

  @Mock private NodeClient client;
  @Mock private ActionFuture<BulkResponse> future;

  private WriteConfig cfg(WriteMode mode, List<String> keyFields, int batchSize) {
    return new WriteConfig(
        "dest", List.of("id", "name"), mode, keyFields, batchSize, RefreshPolicy.NONE);
  }

  private BulkResponse success() {
    return new BulkResponse(new BulkItemResponse[0], 1L);
  }

  @Test
  void batchesAtBatchSizeAndCountsAllRows() {
    when(client.bulk(any(BulkRequest.class))).thenReturn(future);
    when(future.actionGet()).thenReturn(success());

    ArgumentCaptor<BulkRequest> captor = ArgumentCaptor.forClass(BulkRequest.class);
    long written;
    try (OpenSearchBulkWriter writer =
        new OpenSearchBulkWriter(client, cfg(WriteMode.APPEND, List.of(), 1000))) {
      for (int i = 0; i < 2500; i++) {
        writer.add(new Object[] {i, "n" + i});
      }
      written = writer.flush().written();
    }

    // 1000 + 1000 + 500 -> three bulk calls, all rows counted.
    verify(client, times(3)).bulk(captor.capture());
    assertEquals(1000, captor.getAllValues().get(0).numberOfActions());
    assertEquals(500, captor.getAllValues().get(2).numberOfActions());
    assertEquals(2500L, written);
  }

  @Test
  void upsertUsesKeyFieldAsExplicitId() {
    when(client.bulk(any(BulkRequest.class))).thenReturn(future);
    when(future.actionGet()).thenReturn(success());

    ArgumentCaptor<BulkRequest> captor = ArgumentCaptor.forClass(BulkRequest.class);
    try (OpenSearchBulkWriter writer =
        new OpenSearchBulkWriter(client, cfg(WriteMode.UPSERT, List.of("id"), 1000))) {
      writer.add(new Object[] {42, "alice"});
      writer.flush();
    }

    verify(client).bulk(captor.capture());
    IndexRequest req = (IndexRequest) captor.getValue().requests().get(0);
    assertEquals(
        LookupIdEncoder.encode(List.of("id"), List.of("id", "name"), new Object[] {42, "alice"}),
        req.id());
    assertEquals(43, req.id().length());
  }

  @Test
  void appendLeavesIdUnset() {
    when(client.bulk(any(BulkRequest.class))).thenReturn(future);
    when(future.actionGet()).thenReturn(success());

    ArgumentCaptor<BulkRequest> captor = ArgumentCaptor.forClass(BulkRequest.class);
    try (OpenSearchBulkWriter writer =
        new OpenSearchBulkWriter(client, cfg(WriteMode.APPEND, List.of(), 1000))) {
      writer.add(new Object[] {42, "alice"});
      writer.flush();
    }

    verify(client).bulk(captor.capture());
    IndexRequest req = (IndexRequest) captor.getValue().requests().get(0);
    org.junit.jupiter.api.Assertions.assertNull(req.id());
  }

  @Test
  void nonRetryableFailureThrowsInsteadOfSwallowing() {
    BulkItemResponse.Failure failure =
        new BulkItemResponse.Failure(
            "dest", "1", new IllegalArgumentException("mapping error"), RestStatus.BAD_REQUEST);
    BulkItemResponse item = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, failure);
    BulkResponse withFailure = new BulkResponse(new BulkItemResponse[] {item}, 1L);
    when(client.bulk(any(BulkRequest.class))).thenReturn(future);
    when(future.actionGet()).thenReturn(withFailure);

    OpenSearchBulkWriter writer =
        new OpenSearchBulkWriter(client, cfg(WriteMode.APPEND, List.of(), 1000));
    writer.add(new Object[] {1, "a"});
    OpenSearchBulkWriter.BulkWriteException ex =
        assertThrows(OpenSearchBulkWriter.BulkWriteException.class, writer::flush);
    assertEquals(1, ex.getFailures().size());
  }

  @Test
  void upsertConfigRequiresKeyField() {
    assertThrows(
        IllegalArgumentException.class, () -> cfg(WriteMode.UPSERT, List.of(), 1000));
  }
}

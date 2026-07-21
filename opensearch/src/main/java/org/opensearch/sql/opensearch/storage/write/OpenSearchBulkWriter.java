/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.sql.opensearch.storage.write.WriteResult.ItemFailure;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Batched, retrying bulk writer. Pure write-side: it consumes rows, maps them to documents, and
 * indexes them via the native bulk path. It owns no read, PIT, or index-lifecycle concern (create,
 * replace, delete all live in the calling command).
 *
 * <p>It retries only backpressure (429) items under exponential backoff. Any non-429 item failure,
 * or a 429 that survives the retry budget, is raised as a {@link BulkWriteException} rather than
 * silently dropped, so callers never lose rows without knowing.
 */
public final class OpenSearchBulkWriter implements AutoCloseable {

  private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueSeconds(60);

  private final NodeClient client;
  private final WriteConfig cfg;

  private BulkRequest batch = new BulkRequest();
  private int buffered = 0;
  private long written = 0;
  private final List<ItemFailure> tolerated = new ArrayList<>();
  private boolean closed = false;

  public OpenSearchBulkWriter(NodeClient client, WriteConfig cfg) {
    this.client = client;
    this.cfg = cfg;
  }

  /** Buffer a row; when the batch fills, a synchronous bulk flush fires. May throw on failure. */
  public void add(Object[] row) {
    ensureOpen();
    batch.add(toIndexRequest(row));
    if (++buffered >= cfg.batchSize()) {
      flushBatch();
    }
  }

  /** Flush the tail batch and return the cumulative result. */
  public WriteResult flush() {
    ensureOpen();
    if (buffered > 0) {
      flushBatch();
    }
    return new WriteResult(written, List.copyOf(tolerated));
  }

  /** Idempotent: flushes any buffered rows, then blocks further writes. */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    flush();
    closed = true;
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("writer is closed");
    }
  }

  private void flushBatch() {
    BulkRequest pending = batch;
    pending.setRefreshPolicy(cfg.refresh());
    batch = new BulkRequest();
    buffered = 0;

    Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff().iterator();
    long deadlineNanos = System.nanoTime() + RETRY_TIMEOUT.nanos();
    while (true) {
      BulkResponse response = client.bulk(pending).actionGet();
      if (!response.hasFailures()) {
        written += pending.numberOfActions();
        return;
      }

      BulkRequest retry = new BulkRequest();
      retry.setRefreshPolicy(cfg.refresh());
      List<ItemFailure> fatal = new ArrayList<>();
      int succeeded = 0;
      for (BulkItemResponse item : response.getItems()) {
        if (!item.isFailed()) {
          succeeded++;
        } else if (item.getFailure().getStatus() == RestStatus.TOO_MANY_REQUESTS) {
          retry.add(pending.requests().get(item.getItemId()));
        } else {
          fatal.add(new ItemFailure(item.getItemId(), item.getId(), item.getFailureMessage()));
        }
      }
      written += succeeded;

      if (!fatal.isEmpty()) {
        throw new BulkWriteException("outputlookup bulk write hit non-retryable failures", fatal);
      }
      if (retry.numberOfActions() == 0) {
        return;
      }
      if (!backoff.hasNext() || System.nanoTime() >= deadlineNanos) {
        List<ItemFailure> exhausted = new ArrayList<>();
        for (int i = 0; i < retry.numberOfActions(); i++) {
          exhausted.add(new ItemFailure(i, null, "429 retry budget exhausted"));
        }
        throw new BulkWriteException(
            "outputlookup bulk write exhausted 429 retries or hit the "
                + RETRY_TIMEOUT
                + " retry timeout",
            exhausted);
      }
      try {
        Thread.sleep(backoff.next().millis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BulkWriteException("interrupted during bulk retry backoff", List.of());
      }
      pending = retry;
    }
  }

  private IndexRequest toIndexRequest(Object[] row) {
    Map<String, Object> doc = new LinkedHashMap<>();
    List<String> fields = cfg.fields();
    for (int i = 0; i < fields.size() && i < row.length; i++) {
      String name = fields.get(i);
      if (row[i] != null && !OpenSearchIndex.METADATAFIELD_TYPE_MAP.containsKey(name)) {
        doc.put(name, row[i]);
      }
    }
    IndexRequest request = new IndexRequest(cfg.destIndex()).source(doc);
    if (cfg.mode() == WriteMode.UPSERT) {
      request.id(LookupIdEncoder.encode(cfg.keyFields(), fields, row));
    }
    return request;
  }

  /** Raised when a bulk write has failures that were not resolved by retry. Carries the details. */
  public static final class BulkWriteException extends RuntimeException {
    private final transient List<ItemFailure> failures;

    public BulkWriteException(String message, List<ItemFailure> failures) {
      super(message + ": " + failures);
      this.failures = failures;
    }

    public List<ItemFailure> getFailures() {
      return failures;
    }
  }
}

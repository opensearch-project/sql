/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;

/**
 * OpenSearch Bulk writer. More reading https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/.
 * It is not thread safe.
 */
public class OpenSearchWriter extends FlintWriter {

  private final String indexName;

  private final String refreshPolicy;

  private StringBuilder sb;

  private RestHighLevelClient client;

  public OpenSearchWriter(RestHighLevelClient client, String indexName, String refreshPolicy) {
    this.client = client;
    this.indexName = indexName;
    this.sb = new StringBuilder();
    this.refreshPolicy = refreshPolicy;
  }

  @Override public void write(char[] cbuf, int off, int len) {
    sb.append(cbuf, off, len);
  }

  /**
   * Flush the data in buffer.
   * Todo. StringWriter is not efficient. it will copy the cbuf when create bytes.
   */
  @Override public void flush() {
    try {
      if (sb.length() > 0) {
        byte[] bytes = sb.toString().getBytes();
        BulkResponse
            response =
            client.bulk(
                new BulkRequest(indexName).setRefreshPolicy(refreshPolicy).add(bytes, 0, bytes.length, XContentType.JSON),
                RequestOptions.DEFAULT);
        // fail entire bulk request even one doc failed.
        if (response.hasFailures() && Arrays.stream(response.getItems()).anyMatch(itemResp -> !isCreateConflict(itemResp))) {
          throw new RuntimeException(response.buildFailureMessage());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to execute bulk request on index: %s", indexName), e);
    } finally {
      sb.setLength(0);
    }
  }

  @Override public void close() {
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE && (itemResp.getFailure() == null || itemResp.getFailure()
        .getStatus() == RestStatus.CONFLICT);
  }
}



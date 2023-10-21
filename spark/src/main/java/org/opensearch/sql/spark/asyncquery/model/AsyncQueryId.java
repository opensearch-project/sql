/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.sql.spark.utils.IDUtils.decode;
import static org.opensearch.sql.spark.utils.IDUtils.encode;

import lombok.Data;

/**
 * Async query id.
 */
@Data
public class AsyncQueryId {
  private final String id;

  public static AsyncQueryId newAsyncQueryId(String datasourceName) {
    return new AsyncQueryId(encode(datasourceName));
  }

  public String getDataSourceName() {
    return decode(id);
  }

  /** OpenSearch DocId. */
  public String docId() {
    return "qid" + id;
  }

  @Override
  public String toString() {
    return "asyncQueryId=" + id;
  }
}

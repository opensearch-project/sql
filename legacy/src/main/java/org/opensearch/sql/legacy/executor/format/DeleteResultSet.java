/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import java.util.List;
import java.util.Map;
import org.opensearch.client.Client;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.sql.legacy.domain.Delete;

public class DeleteResultSet extends ResultSet {
  private final Delete query;
  private final Object queryResult;

  public static final String DELETED = "deleted_rows";

  public DeleteResultSet(Client client, Delete query, Object queryResult) {
    this.client = client;
    this.query = query;
    this.queryResult = queryResult;
    this.schema = new Schema(loadColumns());
    this.dataRows = new DataRows(loadRows());
  }

  private List<Schema.Column> loadColumns() {
    return List.of(new Schema.Column(DELETED, null, Schema.Type.LONG));
  }

  private List<DataRows.Row> loadRows() {
    return List.of(new DataRows.Row(loadDeletedData()));
  }

  private Map<String, Object> loadDeletedData() {
    return Map.of(DELETED, ((BulkByScrollResponse) queryResult).getDeleted());
  }
}

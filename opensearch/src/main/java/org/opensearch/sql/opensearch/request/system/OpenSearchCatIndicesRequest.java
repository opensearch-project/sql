/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

/** Cat indices request. */
@RequiredArgsConstructor
public class OpenSearchCatIndicesRequest implements OpenSearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "opensearch";

  private static final String DEFAULT_TABLE_TAPE = "BASE TABLE";

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  /**
   * search all the index in the data store.
   *
   * @return list of {@link ExprValue}
   */
  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    final Map<String, String> meta = client.meta();
    for (String index : client.indices()) {
      results.add(row(index, clusterName(meta)));
    }
    return results;
  }

  private ExprTupleValue row(String indexName, String clusterName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CAT", stringValue(clusterName));
    valueMap.put("TABLE_NAME", stringValue(indexName));
    valueMap.put("TABLE_TYPE", stringValue(DEFAULT_TABLE_TAPE));
    return new ExprTupleValue(valueMap);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  @Override
  public String toString() {
    return "OpenSearchCatIndicesRequest{}";
  }
}

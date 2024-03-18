/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.utils.TestUtils;

@Getter
public class MockFlintIndex {
  private final String indexName;
  private final Client client;
  private final FlintIndexType flintIndexType;
  private final String query;

  public MockFlintIndex(
      Client client, String indexName, FlintIndexType flintIndexType, String query) {
    this.client = client;
    this.indexName = indexName;
    this.flintIndexType = flintIndexType;
    this.query = query;
  }

  public void createIndex() {
    String mappingFile = String.format("flint-index-mappings/%s_mapping.json", indexName);
    TestUtils.createIndexWithMappings(client, indexName, mappingFile);
  }

  public String getLatestId() {
    return this.indexName + "_latest_id";
  }

  @SneakyThrows
  public void deleteIndex() {
    client.admin().indices().delete(new DeleteIndexRequest().indices(indexName)).get();
  }

  public Map<String, Object> getIndexMappings() {
    return client
        .admin()
        .indices()
        .prepareGetMappings(indexName)
        .get()
        .getMappings()
        .get(indexName)
        .getSourceAsMap();
  }

  public void updateIndexOptions(HashMap<String, Object> newOptions, Boolean replaceCompletely) {
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings().setIndices(indexName).get();
    Map<String, Object> flintMetadataMap =
        mappingsResponse.getMappings().get(indexName).getSourceAsMap();
    Map<String, Object> meta = (Map<String, Object>) flintMetadataMap.get("_meta");
    Map<String, Object> options = (Map<String, Object>) meta.get("options");
    if (replaceCompletely) {
      meta.put("options", newOptions);
    } else {
      options.putAll(newOptions);
    }
    client.admin().indices().preparePutMapping(indexName).setSource(flintMetadataMap).get();
  }
}

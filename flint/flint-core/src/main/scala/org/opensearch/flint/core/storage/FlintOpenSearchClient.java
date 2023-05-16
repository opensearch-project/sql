/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.metadata.FlintMetadata;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  /** OpenSearch host name. */
  private final String host;

  /** OpenSearch port number. */
  private final int port;

  public FlintOpenSearchClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public void createIndex(String indexName, FlintMetadata metadata) {
    try (RestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(indexName);
      request.mapping(buildIndexMapping(metadata));

      client.indices().create(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Flint index", e);
    }
  }

  @Override
  public boolean exists(String indexName) {
    try (RestHighLevelClient client = createClient()) {
      return client.indices()
          .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists", e);
    }
  }

  @Override
  public FlintMetadata getIndexMetadata(String indexName) {
    try (RestHighLevelClient client = createClient()) {
      GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
      GetMappingsResponse response =
          client.indices().getMapping(request, RequestOptions.DEFAULT);

      return parseIndexMapping(response.mappings().get(indexName));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata", e);
    }
  }

  private RestHighLevelClient createClient() {
    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(host, port, "http")));
  }

  private Map<String, Object> buildIndexMapping(FlintMetadata metadata) {
    // Convert from {"field": "int"} to {"field": {"type": "int"}}
    Map<String, Object> fieldTypes =
        metadata.getSchema().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Map.of("type", entry.getValue())));

    return Map.of(
        "properties", fieldTypes,
        "_meta", metadata.getMeta());
  }

  @SuppressWarnings("unchecked")
  private FlintMetadata parseIndexMapping(MappingMetadata mapping) {
    Map<String, Object> source = mapping.getSourceAsMap();

    // Parse {"field": {"type": "int"}} to {"field": "int"}
    Map<String, String> schema =
        ((Map<String, Object>) source.get("properties"))
            .entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> ((Map<String, String>) entry.getValue()).get("type")));

    return new FlintMetadata(schema, (Map<String, Object>) source.get("_meta"));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  private final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(), new ArrayList<>()).getNamedXContents());

  /** OpenSearch host name. */
  private final String host;

  /** OpenSearch port number. */
  private final int port;

  private final FlintOptions options;

  public FlintOpenSearchClient(FlintOptions options) {
    this.host = options.getHost();
    this.port = options.getPort();
    this.options = options;
  }

  @Override
  public void createIndex(String indexName, FlintMetadata metadata) {
    try (RestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(indexName);
      request.mapping(metadata.getContent(), XContentType.JSON);

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

      MappingMetadata mapping = response.mappings().get(indexName);
      return new FlintMetadata(mapping.source().string());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata", e);
    }
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all.
   * @return {@link FlintReader}.
   */
  @Override public FlintReader createReader(String indexName, String query) {
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      return new OpenSearchScrollReader(createClient(), indexName, new SearchSourceBuilder().query(queryBuilder), options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private RestHighLevelClient createClient() {
    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(host, port, "http")));
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.client;

import com.amazon.opendistroforelasticsearch.sql.opensearch.mapping.IndexMapping;
import com.amazon.opendistroforelasticsearch.sql.opensearch.request.OpenSearchRequest;
import com.amazon.opendistroforelasticsearch.sql.opensearch.response.OpenSearchResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;

/**
 * OpenSearch REST client to support standalone mode that runs entire engine from remote.
 *
 * <p>TODO: Support for authN and authZ with AWS Sigv4 or security plugin.
 */
@RequiredArgsConstructor
public class OpenSearchRestClient implements OpenSearchClient {

  /** OpenSearch high level REST client. */
  private final RestHighLevelClient client;

  @Override
  public Map<String, IndexMapping> getIndexMappings(String indexExpression) {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexExpression);
    try {
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
      return response.mappings().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new IndexMapping(e.getValue())));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get index mappings for " + indexExpression, e);
    }
  }

  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> {
          try {
            return client.search(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform search operation with request " + req, e);
          }
        },
        req -> {
          try {
            return client.scroll(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform scroll operation with request " + req, e);
          }
        }
    );
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    try {
      GetIndexResponse indexResponse =
          client.indices().get(new GetIndexRequest(), RequestOptions.DEFAULT);
      final Stream<String> aliasStream =
          ImmutableList.copyOf(indexResponse.getAliases().values()).stream()
              .flatMap(Collection::stream).map(AliasMetadata::alias);
      return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get indices", e);
    }
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    try {
      final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
      ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
      request.includeDefaults(true);
      request.local(true);
      final Settings defaultSettings =
          client.cluster().getSettings(request, RequestOptions.DEFAULT).getDefaultSettings();
      builder.put(META_CLUSTER_NAME, defaultSettings.get("cluster.name", "opensearch"));
      return builder.build();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get cluster meta info", e);
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    request.clean(scrollId -> {
      try {
        ClearScrollRequest clearRequest = new ClearScrollRequest();
        clearRequest.addScrollId(scrollId);
        client.clearScroll(clearRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to clean up resources for search request " + request, e);
      }
    });

  }

  @Override
  public void schedule(Runnable task) {
    task.run();
  }
}

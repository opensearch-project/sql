/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

package org.opensearch.sql.opensearch.client;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * OpenSearch client abstraction to wrap different OpenSearch client implementation. For
 * example, implementation by node client for OpenSearch plugin or by REST client for
 * standalone mode.
 */
public interface OpenSearchClient {

  String META_CLUSTER_NAME = "CLUSTER_NAME";

  /**
   * Fetch index mapping(s) according to index expression given.
   *
   * @param indexExpression index expression
   * @return index mapping(s) from index name to its mapping
   */
  Map<String, IndexMapping> getIndexMappings(String indexExpression);

  /**
   * Perform search query in the search request.
   *
   * @param request search request
   * @return search response
   */
  OpenSearchResponse search(OpenSearchRequest request);

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  List<String> indices();

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  Map<String, String> meta();

  /**
   * Clean up resources related to the search request, for example scroll context.
   *
   * @param request search request
   */
  void cleanup(OpenSearchRequest request);

  /**
   * Schedule a task to run.
   *
   * @param task task
   */
  void schedule(Runnable task);
}

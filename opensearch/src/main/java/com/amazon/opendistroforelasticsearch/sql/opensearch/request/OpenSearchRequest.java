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
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.request;

import com.amazon.opendistroforelasticsearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.opensearch.response.OpenSearchResponse;
import java.util.function.Consumer;
import java.util.function.Function;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * OpenSearch search request.
 */
public interface OpenSearchRequest {

  /**
   * Apply the search action or scroll action on request based on context.
   *
   * @param searchAction search action.
   * @param scrollAction scroll search action.
   * @return ElasticsearchResponse.
   */
  OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                            Function<SearchScrollRequest, SearchResponse> scrollAction);

  /**
   * Apply the cleanAction on request.
   *
   * @param cleanAction clean action.
   */
  void clean(Consumer<String> cleanAction);

  /**
   * Get the SearchSourceBuilder.
   *
   * @return SearchSourceBuilder.
   */
  SearchSourceBuilder getSourceBuilder();

  /**
   * Get the ElasticsearchExprValueFactory.
   * @return ElasticsearchExprValueFactory.
   */
  OpenSearchExprValueFactory getExprValueFactory();
}

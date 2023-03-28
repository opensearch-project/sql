/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * Builds a {@link ContinuePageRequest} to handle subsequent pagination/scroll/cursor requests.
 * Initial search requests is handled by {@link InitialPageRequestBuilder}.
 */
@RequiredArgsConstructor
public class ContinuePageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final String scrollId;
  private final OpenSearchExprValueFactory exprValueFactory;

  @Override
  public OpenSearchRequest build() {
    return new ContinuePageRequest(scrollId, exprValueFactory);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import org.opensearch.common.unit.TimeValue;

/**
 * Builds a {@link ContinuePageRequest} to handle subsequent pagination/scroll/cursor requests.
 * Initial search requests are handled by {@link OpenSearchRequestBuilder}
 */
public class ContinuePageRequestBuilder implements ExecutableRequestBuilder {

  @Override
  public  OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                                  int maxResultWindow, TimeValue scrollTimeout) {
    throw new UnsupportedOperationException("Implement me");
    //return new OpenSearchScrollRequest(scrollId, scrollTimeout, exprValueFactory);
  }

}

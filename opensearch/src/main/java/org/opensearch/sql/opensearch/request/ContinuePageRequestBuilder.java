/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.Getter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * Builds a {@link ContinuePageRequest} to handle subsequent pagination/scroll/cursor requests.
 * Initial search requests are handled by {@link OpenSearchRequestBuilder}
 */
public class ContinuePageRequestBuilder implements ExecutableRequestBuilder {

  @Getter
  private final String scrollId;
  private final OpenSearchExprValueFactory exprValueFactory;
  private final TimeValue scrollTimeout;

  /** Constructor. */
  public ContinuePageRequestBuilder(String scrollId, TimeValue scrollTimeout,
                                    OpenSearchExprValueFactory exprValueFactory) {
    this.scrollId = scrollId;
    this.scrollTimeout = scrollTimeout;
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public  OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                                  int maxResultWindow,
                                  Settings settings) {
    return new ContinuePageRequest(scrollId, scrollTimeout, exprValueFactory);
  }

  @Override
  public int getMaxResponseSize() {
    return Integer.MAX_VALUE;
  }
}

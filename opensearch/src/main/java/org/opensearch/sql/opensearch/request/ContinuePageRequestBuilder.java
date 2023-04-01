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
 * Initial search requests is handled by {@link InitialPageRequestBuilder}.
 */
public class ContinuePageRequestBuilder extends PagedRequestBuilder {

  @Getter
  private final OpenSearchRequest.IndexName indexName;
  private final String scrollId;
  private final TimeValue scrollTimeout;
  private final OpenSearchExprValueFactory exprValueFactory;

  /** Constructor. */
  public ContinuePageRequestBuilder(OpenSearchRequest.IndexName indexName,
                                    String scrollId,
                                    Settings settings,
                                    OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.scrollId = scrollId;
    this.scrollTimeout = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public OpenSearchRequest build() {
    return new ContinuePageRequest(scrollId, scrollTimeout, exprValueFactory);
  }
}

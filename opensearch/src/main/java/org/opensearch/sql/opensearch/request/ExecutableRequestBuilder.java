package org.opensearch.sql.opensearch.request;

import org.opensearch.sql.common.setting.Settings;

public interface ExecutableRequestBuilder {
  int getMaxResponseSize();

  OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                          int maxResultWindow,
                          Settings settings);
}

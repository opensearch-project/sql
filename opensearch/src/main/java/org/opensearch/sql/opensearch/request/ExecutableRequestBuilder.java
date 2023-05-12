package org.opensearch.sql.opensearch.request;

import org.opensearch.common.unit.TimeValue;

public interface ExecutableRequestBuilder {

  OpenSearchRequest build(OpenSearchRequest.IndexName indexName,
                          int maxResultWindow, TimeValue scrollTimeout);
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

public abstract class PagedRequestBuilder implements PushDownRequestBuilder {
  public abstract OpenSearchRequest build();

  public abstract OpenSearchRequest.IndexName getIndexName();
}

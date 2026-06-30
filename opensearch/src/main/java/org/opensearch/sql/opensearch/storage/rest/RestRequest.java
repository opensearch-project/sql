/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * Dispatches an allow-listed, read-only management endpoint through the transport node client under
 * the caller's security thread-context and returns the response shaped to the endpoint's fixed
 * schema. The {@code rest} analogue of {@code OpenSearchCatIndicesRequest}; it implements {@link
 * OpenSearchSystemRequest} so the enumerator pattern (resource-monitored iteration) is identical to
 * the system-index scan family.
 */
public class RestRequest implements OpenSearchSystemRequest {

  private final OpenSearchClient client;
  private final RestEndpointRegistry.Endpoint endpoint;
  private final RestSpec spec;

  public RestRequest(
      OpenSearchClient client, RestEndpointRegistry.Endpoint endpoint, RestSpec spec) {
    this.client = client;
    this.endpoint = endpoint;
    this.spec = spec;
  }

  @Override
  public List<ExprValue> search() {
    List<ExprValue> rows = endpoint.toRows(client, spec);
    if (spec.getCount() != null && spec.getCount() >= 0 && rows.size() > spec.getCount()) {
      return rows.subList(0, spec.getCount());
    }
    return rows;
  }

  @Override
  public String toString() {
    return "RestRequest{endpoint=" + endpoint.getPath() + "}";
  }
}

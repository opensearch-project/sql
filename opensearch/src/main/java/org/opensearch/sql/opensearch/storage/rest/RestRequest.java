/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Dispatches an allow-listed, read-only management endpoint through the endpoint's handler under
 * the caller's security thread-context and returns the response shaped to the endpoint's fixed
 * schema. The {@code rest} analogue of {@code OpenSearchCatIndicesRequest}; it implements {@link
 * OpenSearchSystemRequest} so the enumerator pattern (resource-monitored iteration) is identical to
 * the system-index scan family. This is the lazy scan: the handler runs here at {@link #search}
 * (execution), never at planning time.
 */
public class RestRequest implements OpenSearchSystemRequest {

  private final OpenSearchClient client;
  private final RestEndpointRegistry.Endpoint endpoint;
  private final RestSpec spec;
  private final RedactionRegistry redaction;

  public RestRequest(
      OpenSearchClient client, RestEndpointRegistry.Endpoint endpoint, RestSpec spec) {
    this(client, endpoint, spec, new RedactionRegistry());
  }

  public RestRequest(
      OpenSearchClient client,
      RestEndpointRegistry.Endpoint endpoint,
      RestSpec spec,
      RedactionRegistry redaction) {
    this.client = client;
    this.endpoint = endpoint;
    this.spec = spec;
    this.redaction = redaction;
  }

  @Override
  public List<ExprValue> search() {
    // The context's transport client is only used by externally contributed providers; the
    // built-in provider's handlers hold their own client. Sourced from the same OpenSearchClient
    // the storage engine uses, so it runs under the caller's security thread-context.
    NodeClient nodeClient = client == null ? null : client.getNodeClient().orElse(null);
    RestEndpointContext ctx = RestEndpointContext.of(spec.getArgs(), nodeClient);
    List<ExprValue> rows = endpoint.toRows(ctx, redaction);
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

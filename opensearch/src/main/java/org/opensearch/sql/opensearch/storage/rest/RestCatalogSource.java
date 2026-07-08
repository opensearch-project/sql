/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.Map;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.opensearch.storage.system.CatalogSource;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * {@link CatalogSource} for the {@code rest} command: an allow-listed, read-only management
 * endpoint resolved against {@link RestEndpointRegistry}, exposing the fixed endpoint schema.
 * Calcite only (no V2 path) and {@code Scannable} for the {@code collect} short-circuit.
 */
@Getter
public class RestCatalogSource implements CatalogSource {

  private final OpenSearchClient client;
  private final RestSpec spec;
  private final RestEndpointRegistry.Endpoint endpoint;
  private final boolean redact;

  public RestCatalogSource(OpenSearchClient client, RestSpec spec) {
    this(client, spec, false);
  }

  public RestCatalogSource(OpenSearchClient client, RestSpec spec, boolean redact) {
    this.client = client;
    this.spec = spec;
    this.redact = redact;
    // Allow-list enforced here: unknown or mutating endpoints and disallowed args are rejected.
    this.endpoint = RestEndpointRegistry.resolve(spec.getEndpoint());
    RestEndpointRegistry.validate(spec);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return endpoint.getSchema();
  }

  @Override
  public OpenSearchSystemRequest createRequest() {
    return new RestRequest(client, endpoint, spec, redact);
  }

  @Override
  public boolean isScannable() {
    return true;
  }

  @Override
  public PhysicalPlan implementV2(LogicalPlan plan) {
    throw new UnsupportedOperationException("rest command is supported only on the Calcite engine");
  }
}

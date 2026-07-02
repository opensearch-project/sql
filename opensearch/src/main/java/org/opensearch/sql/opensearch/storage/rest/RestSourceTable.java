/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.plan.AbstractOpenSearchTable;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * The {@code rest} command's row source: a fixed-schema, server-side dispatch table modeled on
 * {@link org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex}. It resolves the
 * validated endpoint spec against {@link RestEndpointRegistry} (which enforces the read-only
 * allow-list), exposes the endpoint's fixed schema, and produces a {@link CalciteLogicalRestScan}
 * on the Calcite path.
 */
@Getter
public class RestSourceTable extends AbstractOpenSearchTable {

  private final OpenSearchClient client;
  private final Settings settings;
  private final RestSpec spec;
  private final RestEndpointRegistry.Endpoint endpoint;

  public RestSourceTable(OpenSearchClient client, Settings settings, RestSpec spec) {
    this.client = client;
    this.settings = settings;
    this.spec = spec;
    // Allow-list enforced here: unknown/mutating endpoints and disallowed args
    // are rejected.
    this.endpoint = RestEndpointRegistry.resolve(spec.getEndpoint());
    RestEndpointRegistry.validate(spec);
  }

  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException("rest endpoint is predefined and cannot be created");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return endpoint.getSchema();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CalciteLogicalRestScan(cluster, relOptTable, this);
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    throw new UnsupportedOperationException("rest command is supported only on the Calcite engine");
  }

  public RestRequest createRestRequest() {
    return new RestRequest(client, endpoint, spec);
  }

  public OpenSearchResourceMonitor createOpenSearchResourceMonitor() {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy(settings));
  }
}

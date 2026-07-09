/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.collect;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.plugin.config.EngineExtensionsHolder;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Runs a terminal {@code collect} query's write in the background. Builds the PPL execution stack
 * (mirroring {@code TransportPPLQueryAction}) and calls {@code PPLService.execute} directly rather
 * than dispatching {@code PPLQueryAction}, so it bypasses the foreground collect-routing and cannot
 * recurse. The collect plan's streaming write operator persists rows to the destination as a side
 * effect; the returned rows are discarded. The outcome is stored to {@code .tasks} because the
 * request sets {@code shouldStoreResult=true}.
 */
public class TransportCollectMaterializeAction
    extends HandledTransportAction<CollectMaterializeRequest, CollectMaterializeResponse> {

  private final Injector injector;

  @Inject
  public TransportCollectMaterializeAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      EngineExtensionsHolder extensionsHolder) {
    super(
        CollectMaterializeAction.NAME,
        transportService,
        actionFilters,
        CollectMaterializeRequest::new);
    this.injector =
        org.opensearch.sql.plugin.config.PplInjectorBuilder.build(
            client, clusterService, dataSourceService, extensionsHolder);
  }

  @Override
  protected void doExecute(
      Task task,
      CollectMaterializeRequest request,
      ActionListener<CollectMaterializeResponse> listener) {
    PPLService pplService = injector.getInstance(PPLService.class);
    // Format is irrelevant: we consume the raw QueryResponse ourselves and never format it.
    PPLQueryRequest pplRequest =
        new TransportPPLQueryRequest(request.getPplQuery(), null, "").toPPLQueryRequest();
    pplService.execute(
        pplRequest,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            // The write already happened as a side effect of executing the collect plan.
            listener.onResponse(new CollectMaterializeResponse(request.getIndexName(), true));
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        },
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {}

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }
}

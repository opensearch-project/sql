/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.collect;

import org.opensearch.action.ActionType;

/**
 * Transport action for async {@code collect} materialization. The foreground detects a terminal
 * {@code | collect} query, returns a capped preview, and submits this action to run the full
 * collect (write) in the background. Registered via {@code ActionPlugin.getActions()} and invoked
 * with {@code shouldStoreResult=true} so its result persists to the {@code .tasks} system index,
 * making status and cancellation available through the core {@code _tasks} API.
 */
public class CollectMaterializeAction extends ActionType<CollectMaterializeResponse> {

  public static final String NAME = "cluster:admin/opensearch/ppl/collect/materialize";
  public static final CollectMaterializeAction INSTANCE = new CollectMaterializeAction();

  private CollectMaterializeAction() {
    super(NAME, CollectMaterializeResponse::new);
  }
}

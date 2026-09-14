/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/** Retrieves the current state and retained rows of one PPL asynchronous job. */
public class PPLAsyncQueryResultAction extends ActionType<TransportPPLQueryResponse> {
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/result";
  public static final PPLAsyncQueryResultAction INSTANCE = new PPLAsyncQueryResultAction();

  private PPLAsyncQueryResultAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}

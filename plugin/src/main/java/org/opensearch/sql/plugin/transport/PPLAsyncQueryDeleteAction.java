/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/** Cancels, if necessary, and releases one retained PPL asynchronous job. */
public class PPLAsyncQueryDeleteAction extends ActionType<TransportPPLQueryResponse> {
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/delete";
  public static final PPLAsyncQueryDeleteAction INSTANCE = new PPLAsyncQueryDeleteAction();

  private PPLAsyncQueryDeleteAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}

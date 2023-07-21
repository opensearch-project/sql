/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

public class PPLQueryAction extends ActionType<TransportPPLQueryResponse> {
  // Internal Action which is not used for public facing RestAPIs.
  public static final String NAME = "cluster:admin/opensearch/ppl";
  public static final PPLQueryAction INSTANCE = new PPLQueryAction();

  private PPLQueryAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}

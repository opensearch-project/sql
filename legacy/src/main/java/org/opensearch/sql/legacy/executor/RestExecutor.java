/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import java.util.Map;
import org.opensearch.client.Client;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.legacy.query.QueryAction;

/** Created by Eliran on 26/12/2015. */
public interface RestExecutor {
  void execute(
      Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel)
      throws Exception;

  String execute(Client client, Map<String, String> params, QueryAction queryAction)
      throws Exception;
}

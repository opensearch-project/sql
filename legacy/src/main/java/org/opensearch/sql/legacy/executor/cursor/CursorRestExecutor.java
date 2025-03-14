/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.cursor;

import java.util.Map;
import org.opensearch.rest.RestChannel;
import org.opensearch.transport.client.Client;

/** Interface to execute cursor request. */
public interface CursorRestExecutor {

  void execute(Client client, Map<String, String> params, RestChannel channel) throws Exception;

  String execute(Client client, Map<String, String> params) throws Exception;
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.executor.cursor;

import java.util.Map;
import org.opensearch.client.Client;
import org.opensearch.rest.RestChannel;

/**
 * Interface to execute cursor request.
 */
public interface CursorRestExecutor {

    void execute(Client client, Map<String, String> params, RestChannel channel)
            throws Exception;

    String execute(Client client, Map<String, String> params) throws Exception;
}

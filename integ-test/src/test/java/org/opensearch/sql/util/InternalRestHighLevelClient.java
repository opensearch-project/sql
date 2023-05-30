/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.util.Collections;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

/**
 * Internal RestHighLevelClient only for testing purpose.
 */
public class InternalRestHighLevelClient extends RestHighLevelClient {
  public InternalRestHighLevelClient(RestClient restClient) {
    super(restClient, RestClient::close, Collections.emptyList());
  }
}

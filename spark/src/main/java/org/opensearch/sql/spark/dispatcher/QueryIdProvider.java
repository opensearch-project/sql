/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/** Interface for extension point to specify queryId. Called when new query is executed. */
public interface QueryIdProvider {
  String getQueryId(DispatchQueryRequest dispatchQueryRequest);
}

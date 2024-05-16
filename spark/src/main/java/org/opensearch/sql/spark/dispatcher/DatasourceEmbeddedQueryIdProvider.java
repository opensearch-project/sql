/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/** Generates QueryId by embedding Datasource name and random UUID */
public class DatasourceEmbeddedQueryIdProvider implements QueryIdProvider {

  @Override
  public String getQueryId(DispatchQueryRequest dispatchQueryRequest) {
    return AsyncQueryId.newAsyncQueryId(dispatchQueryRequest.getDatasource()).getId();
  }
}

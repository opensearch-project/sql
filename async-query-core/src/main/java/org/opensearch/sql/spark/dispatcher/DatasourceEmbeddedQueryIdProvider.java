/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.utils.IDUtils;

/** Generates QueryId by embedding Datasource name and random UUID */
public class DatasourceEmbeddedQueryIdProvider implements QueryIdProvider {

  @Override
  public String getQueryId(
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return IDUtils.encode(dispatchQueryRequest.getDatasource());
  }
}

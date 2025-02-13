/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query;

import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.opensearch.sql.legacy.domain.IndexStatement;
import org.opensearch.sql.legacy.domain.QueryStatement;
import org.opensearch.sql.legacy.utils.Util;
import org.opensearch.transport.client.Client;

public class DescribeQueryAction extends QueryAction {

  private final IndexStatement statement;

  public DescribeQueryAction(Client client, IndexStatement statement) {
    super(client, null);
    this.statement = statement;
  }

  @Override
  public QueryStatement getQueryStatement() {
    return statement;
  }

  @Override
  public SqlOpenSearchRequestBuilder explain() {
    final GetIndexRequestBuilder indexRequestBuilder =
        Util.prepareIndexRequestBuilder(client, statement);

    return new SqlOpenSearchRequestBuilder(indexRequestBuilder);
  }
}

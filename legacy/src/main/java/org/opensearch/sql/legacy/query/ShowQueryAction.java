/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query;

import static org.opensearch.sql.legacy.utils.Util.prepareIndexRequestBuilder;

import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.IndexStatement;
import org.opensearch.sql.legacy.domain.QueryStatement;

public class ShowQueryAction extends QueryAction {

  private final IndexStatement statement;

  public ShowQueryAction(Client client, IndexStatement statement) {
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
        prepareIndexRequestBuilder(client, statement);

    return new SqlOpenSearchRequestBuilder(indexRequestBuilder);
  }
}

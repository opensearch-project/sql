/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;
import org.opensearch.sql.legacy.domain.QueryActionRequest;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.transport.client.Client;

public class SearchDao {

  private static final Set<String> END_TABLE_MAP = new HashSet<>();

  static {
    END_TABLE_MAP.add("limit");
    END_TABLE_MAP.add("order");
    END_TABLE_MAP.add("where");
    END_TABLE_MAP.add("group");
  }

  private Client client = null;

  public SearchDao(Client client) {
    this.client = client;
  }

  public Client getClient() {
    return client;
  }

  /**
   * Prepare action And transform sql into OpenSearch ActionRequest
   *
   * @param queryActionRequest SQL query action request to execute.
   * @return OpenSearch request
   * @throws SqlParseException
   */
  public QueryAction explain(QueryActionRequest queryActionRequest)
      throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureDisabledException {
    return OpenSearchActionFactory.create(client, queryActionRequest, false);
  }
}

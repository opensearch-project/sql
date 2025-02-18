/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.multi;

import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.transport.client.Client;

/** Created by Eliran on 19/8/2016. */
public class OpenSearchMultiQueryActionFactory {

  public static QueryAction createMultiQueryAction(Client client, MultiQuerySelect multiSelect)
      throws SqlParseException {
    switch (multiSelect.getOperation()) {
      case UNION_ALL:
      case UNION:
        return new MultiQueryAction(client, multiSelect);
      default:
        throw new SqlParseException("only supports union and union all");
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import com.alibaba.druid.sql.parser.ParserException;
import java.sql.SQLFeatureNotSupportedException;
import org.mockito.Mockito;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.transport.client.Client;

/** Test utils class that explains a query */
public class SqlExplainUtils {

  public static String explain(String query) {
    try {
      Client mockClient = Mockito.mock(Client.class);
      CheckScriptContents.stubMockClient(mockClient);
      QueryAction queryAction = OpenSearchActionFactory.create(mockClient, query);

      return queryAction.explain().explain();
    } catch (SqlParseException | SQLFeatureNotSupportedException | SQLFeatureDisabledException e) {
      throw new ParserException("Illegal sql expr in: " + query);
    }
  }

  private SqlExplainUtils() {}
}

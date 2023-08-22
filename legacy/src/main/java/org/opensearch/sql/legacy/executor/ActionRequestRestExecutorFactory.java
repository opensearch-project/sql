/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import org.opensearch.sql.legacy.executor.csv.CSVResultRestExecutor;
import org.opensearch.sql.legacy.executor.format.PrettyFormatRestExecutor;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.join.OpenSearchJoinQueryAction;
import org.opensearch.sql.legacy.query.multi.MultiQueryAction;

/** Created by Eliran on 26/12/2015. */
public class ActionRequestRestExecutorFactory {
  /**
   * Create executor based on the format and wrap with AsyncRestExecutor to async blocking execute()
   * call if necessary.
   *
   * @param format format of response
   * @param queryAction query action
   * @return executor
   */
  public static RestExecutor createExecutor(Format format, QueryAction queryAction) {
    switch (format) {
      case CSV:
        return new AsyncRestExecutor(new CSVResultRestExecutor());
      case JSON:
        return new AsyncRestExecutor(
            new ElasticDefaultRestExecutor(queryAction),
            action -> isJoin(action) || isUnionMinus(action));
      case JDBC:
      case RAW:
      case TABLE:
      default:
        return new AsyncRestExecutor(new PrettyFormatRestExecutor(format.getFormatName()));
    }
  }

  private static boolean isJoin(QueryAction queryAction) {
    return queryAction instanceof OpenSearchJoinQueryAction;
  }

  private static boolean isUnionMinus(QueryAction queryAction) {
    return queryAction instanceof MultiQueryAction;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.utils;

import static org.opensearch.sql.legacy.utils.Util.toSqlExpr;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.legacy.rewriter.identifier.AnonymizeSensitiveDataRule;

/** Utility class to mask sensitive information in incoming SQL queries */
public class QueryDataAnonymizer {

  private static final Logger LOG = LogManager.getLogger(QueryDataAnonymizer.class);

  /**
   * This method is used to anonymize sensitive data in SQL query. Sensitive data includes index
   * names, column names etc., which in druid parser are parsed to SQLIdentifierExpr instances
   *
   * @param query entire sql query string
   * @return sql query string with all identifiers replaced with "***" on success and failure string
   *     otherwise to ensure no non-anonymized data is logged in production.
   */
  public static String anonymizeData(String query) {
    String resultQuery;
    try {
      AnonymizeSensitiveDataRule rule = new AnonymizeSensitiveDataRule();
      SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(query);
      rule.rewrite(sqlExpr);
      resultQuery =
          SQLUtils.toMySqlString(sqlExpr)
              .replaceAll("0", "number")
              .replaceAll("false", "boolean_literal")
              .replaceAll("[\\n][\\t]+", " ");
    } catch (Exception e) {
      LOG.warn("Caught an exception when anonymizing sensitive data.");
      LOG.debug("String {} failed anonymization.", query);
      resultQuery = "Failed to anonymize data.";
    }
    return resultQuery;
  }
}

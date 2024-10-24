/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** Validate input SQL query based on the DataSourceType. */
@AllArgsConstructor
public class SQLQueryValidator {
  private static final Logger log = LogManager.getLogger(SQLQueryValidator.class);

  private final GrammarElementValidatorProvider grammarElementValidatorProvider;

  /**
   * It will look up validator associated with the DataSourceType, and throw
   * IllegalArgumentException if invalid grammar element is found.
   *
   * @param sqlQuery The query to be validated
   * @param datasourceType
   */
  public void validate(String sqlQuery, DataSourceType datasourceType) {
    GrammarElementValidator grammarElementValidator =
        grammarElementValidatorProvider.getValidatorForDatasource(datasourceType);
    SQLQueryValidationVisitor visitor = new SQLQueryValidationVisitor(grammarElementValidator);
    try {
      visitor.visit(SQLQueryUtils.getBaseParser(sqlQuery).singleStatement());
    } catch (IllegalArgumentException e) {
      log.error("Query validation failed. DataSourceType=" + datasourceType, e);
      throw e;
    }
  }

  public void validateFlintExtensionQuery(String sqlQuery, DataSourceType dataSourceType) {}
}

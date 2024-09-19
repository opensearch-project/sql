/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** Validate input SQL query based on the DataSourceType. */
@AllArgsConstructor
public class SQLQueryValidator {
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
    visitor.visit(SQLQueryUtils.getBaseParser(sqlQuery).singleStatement());
  }
}

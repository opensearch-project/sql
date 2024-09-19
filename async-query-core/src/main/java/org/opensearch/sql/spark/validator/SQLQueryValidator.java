/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

@AllArgsConstructor
public class SQLQueryValidator {
  private final GrammarElementValidatorProvider grammarElementValidatorProvider;

  public void validate(String sqlQuery, DataSourceType datasourceType) {
    GrammarElementValidator grammarElementValidator =
        grammarElementValidatorProvider.getValidatorForDatasource(datasourceType);
    SQLQueryValidationVisitor visitor = new SQLQueryValidationVisitor(grammarElementValidator);
    visitor.visit(SQLQueryUtils.getBaseParser(sqlQuery).singleStatement());
  }
}

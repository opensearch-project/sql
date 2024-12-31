/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import lombok.AllArgsConstructor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.spark.antlr.parser.OpenSearchPPLParser;

@AllArgsConstructor
public class PPLQueryValidator {
  private static final Logger log = LogManager.getLogger(SQLQueryValidator.class);

  private final GrammarElementValidatorProvider grammarElementValidatorProvider;

  /**
   * It will look up validator associated with the DataSourceType, and throw
   * IllegalArgumentException if invalid grammar element is found.
   *
   * @param pplQuery The query to be validated
   * @param datasourceType
   */
  public void validate(String pplQuery, DataSourceType datasourceType) {
    GrammarElementValidator grammarElementValidator =
        grammarElementValidatorProvider.getValidatorForDatasource(datasourceType);
    PPLQueryValidationVisitor visitor = new PPLQueryValidationVisitor(grammarElementValidator);
    try {
      visitor.visit(getPplParser(pplQuery).root());
    } catch (IllegalArgumentException e) {
      log.error("Query validation failed. DataSourceType=" + datasourceType, e);
      throw e;
    }
  }

  public static OpenSearchPPLParser getPplParser(String pplQuery) {
    OpenSearchPPLParser sqlBaseParser =
        new OpenSearchPPLParser(
            new CommonTokenStream(new OpenSearchPPLLexer(new CaseInsensitiveCharStream(pplQuery))));
    sqlBaseParser.addErrorListener(new SyntaxAnalysisErrorListener());
    return sqlBaseParser;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

/** Calcite SQL query parser that produces {@link SqlNode} as the native parse result. */
@RequiredArgsConstructor
public class CalciteSqlQueryParser implements UnifiedQueryParser<SqlNode> {

  /** Calcite plan context providing parser configuration (e.g., case sensitivity, conformance). */
  private final CalcitePlanContext planContext;

  @Override
  public SqlNode parse(String query) {
    try {
      SqlParser parser = SqlParser.create(query, planContext.config.getParserConfig());
      return parser.parseQuery();
    } catch (SqlParseException e) {
      throw new SyntaxCheckException("Failed to parse SQL query: " + e.getMessage());
    }
  }
}

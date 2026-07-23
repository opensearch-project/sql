/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import org.opensearch.sql.ast.expression.SearchExpression;
import org.opensearch.sql.calcite.SearchPredicateCompiler;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser.SearchPredicateContext;

/** Uses the PPL search-expression grammar to compile an implicit format result. */
public class PPLSearchPredicateCompiler implements SearchPredicateCompiler {

  private final PPLSyntaxParser syntaxParser = new PPLSyntaxParser();

  @Override
  public String compile(String predicate) {
    String trimmed = predicate == null ? "" : predicate.trim();
    if (trimmed.isEmpty()) {
      return "*:*";
    }
    if (trimmed.matches("(?i)NOT\\s*\\(\\s*\\)")) {
      return "*:* AND NOT *:*";
    }

    SearchPredicateContext parsed = syntaxParser.parseSearchPredicate(trimmed);
    AstBuilder astBuilder = new AstBuilder(trimmed);
    SearchExpression expression =
        (SearchExpression) new AstExpressionBuilder(astBuilder).visit(parsed.searchExpression());
    return expression.toQueryString();
  }
}

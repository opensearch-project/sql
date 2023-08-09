/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprType;

class QualifierAnalyzerTest extends AnalyzerTestBase {

  private QualifierAnalyzer qualifierAnalyzer;

  @BeforeEach
  void setUp() {
    qualifierAnalyzer = new QualifierAnalyzer(analysisContext);
  }

  @Test
  void should_return_original_name_if_no_qualifier() {
    assertEquals("integer_value", qualifierAnalyzer.unqualified("integer_value"));
  }

  @Test
  void should_report_error_if_qualifier_is_not_index() {
    runInScope(
        new Symbol(Namespace.FIELD_NAME, "aIndex"),
        ARRAY,
        () -> {
          SyntaxCheckException error =
              assertThrows(
                  SyntaxCheckException.class,
                  () -> qualifierAnalyzer.unqualified("a", "integer_value"));
          assertEquals(
              "The qualifier [a] of qualified name [a.integer_value] "
                  + "must be an field name, index name or its alias",
              error.getMessage());
        });
  }

  @Test
  void should_report_error_if_qualifier_is_not_exist() {
    SyntaxCheckException error =
        assertThrows(
            SyntaxCheckException.class, () -> qualifierAnalyzer.unqualified("a", "integer_value"));
    assertEquals(
        "The qualifier [a] of qualified name [a.integer_value] must be an field name, index name "
            + "or its alias",
        error.getMessage());
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_index() {
    runInScope(
        new Symbol(Namespace.INDEX_NAME, "a"),
        STRUCT,
        () -> assertEquals("integer_value", qualifierAnalyzer.unqualified("a", "integer_value")));
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_field() {
    runInScope(
        new Symbol(Namespace.FIELD_NAME, "a"),
        STRUCT,
        () -> assertEquals("a.integer_value", qualifierAnalyzer.unqualified("a", "integer_value")));
  }

  @Test
  void should_report_error_if_more_parts_in_qualified_name() {
    runInScope(
        new Symbol(Namespace.INDEX_NAME, "a"),
        STRUCT,
        () -> qualifierAnalyzer.unqualified("a", "integer_value", "invalid"));
  }

  private void runInScope(Symbol symbol, ExprType type, Runnable test) {
    analysisContext.push();
    analysisContext.peek().define(symbol, type);
    try {
      test.run();
    } finally {
      analysisContext.pop();
    }
  }
}

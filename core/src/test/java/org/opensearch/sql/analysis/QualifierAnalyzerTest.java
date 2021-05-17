/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
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
    runInScope(new Symbol(Namespace.FIELD_NAME, "aIndex"), ARRAY, () -> {
      SyntaxCheckException error = assertThrows(SyntaxCheckException.class,
          () -> qualifierAnalyzer.unqualified("a", "integer_value"));
      assertEquals("The qualifier [a] of qualified name [a.integer_value] "
              + "must be an field name, index name or its alias", error.getMessage());
    });
  }

  @Test
  void should_report_error_if_qualifier_is_not_exist() {
    SyntaxCheckException error = assertThrows(SyntaxCheckException.class,
        () -> qualifierAnalyzer.unqualified("a", "integer_value"));
    assertEquals(
        "The qualifier [a] of qualified name [a.integer_value] must be an field name, index name "
            + "or its alias",
        error.getMessage());
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_index() {
    runInScope(new Symbol(Namespace.INDEX_NAME, "a"), STRUCT, () ->
        assertEquals("integer_value", qualifierAnalyzer.unqualified("a", "integer_value"))
    );
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_field() {
    runInScope(new Symbol(Namespace.FIELD_NAME, "a"), STRUCT, () ->
        assertEquals("a.integer_value", qualifierAnalyzer.unqualified("a", "integer_value"))
    );
  }

  @Test
  void should_report_error_if_more_parts_in_qualified_name() {
    runInScope(new Symbol(Namespace.INDEX_NAME, "a"), STRUCT, () ->
        qualifierAnalyzer.unqualified("a", "integer_value", "invalid")
    );
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

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

package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.RuleNode;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

public class AstQualifiedNameBuilderTest {

  @Test
  public void canBuildRegularIdentifierForSQLStandard() {
    buildFromIdentifier("test").expectQualifiedName("test");
    buildFromIdentifier("test123").expectQualifiedName("test123");
    buildFromIdentifier("test_123").expectQualifiedName("test_123");
  }

  @Test
  public void canBuildRegularIdentifierForOpenSearch() {
    buildFromTableName(".opensearch_dashboards").expectQualifiedName(".opensearch_dashboards");
    buildFromIdentifier("@timestamp").expectQualifiedName("@timestamp");
    buildFromIdentifier("logs-2020-01").expectQualifiedName("logs-2020-01");
    buildFromIdentifier("*logs*").expectQualifiedName("*logs*");
  }

  @Test
  public void canBuildDelimitedIdentifier() {
    buildFromIdentifier("`logs.2020.01`").expectQualifiedName("logs.2020.01");
  }

  @Test
  public void canBuildQualifiedIdentifier() {
    buildFromQualifiers("account.location.city").expectQualifiedName("account", "location", "city");
  }

  private AstExpressionBuilderAssertion buildFromIdentifier(String expr) {
    return new AstExpressionBuilderAssertion(OpenSearchSQLParser::ident, expr);
  }

  private AstExpressionBuilderAssertion buildFromQualifiers(String expr) {
    return new AstExpressionBuilderAssertion(OpenSearchSQLParser::qualifiedName, expr);
  }

  private AstExpressionBuilderAssertion buildFromTableName(String expr) {
    return new AstExpressionBuilderAssertion(OpenSearchSQLParser::tableName, expr);
  }

  @RequiredArgsConstructor
  private static class AstExpressionBuilderAssertion {
    private final AstExpressionBuilder astExprBuilder = new AstExpressionBuilder();
    private final Function<OpenSearchSQLParser, RuleNode> build;
    private final String actual;

    public void expectQualifiedName(String... expected) {
      assertEquals(AstDSL.qualifiedName(expected), buildExpression(actual));
    }

    private UnresolvedExpression buildExpression(String expr) {
      return build.apply(createParser(expr)).accept(astExprBuilder);
    }

    private OpenSearchSQLParser createParser(String expr) {
      OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(expr));
      OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
      parser.addErrorListener(new SyntaxAnalysisErrorListener());
      return parser;
    }
  }

}

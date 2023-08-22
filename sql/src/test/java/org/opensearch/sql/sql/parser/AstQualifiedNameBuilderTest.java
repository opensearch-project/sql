/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  @Test
  public void commonKeywordCanBeUsedAsIdentifier() {
    buildFromIdentifier("type").expectQualifiedName("type");
  }

  @Test
  public void functionNameCanBeUsedAsIdentifier() {
    assertFunctionNameCouldBeId("AVG | COUNT | SUM | MIN | MAX");
    assertFunctionNameCouldBeId(
        "CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | LOCALTIME | LOCALTIMESTAMP |"
            + " UTC_TIMESTAMP | UTC_DATE | UTC_TIME | CURDATE | CURTIME | NOW");
  }

  void assertFunctionNameCouldBeId(String antlrFunctionName) {
    List<String> functionList =
        Arrays.stream(antlrFunctionName.split("\\|"))
            .map(String::stripLeading)
            .map(String::stripTrailing)
            .collect(Collectors.toList());

    assertFalse(functionList.isEmpty());
    for (String functionName : functionList) {
      buildFromQualifiers(functionName).expectQualifiedName(functionName);
    }
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

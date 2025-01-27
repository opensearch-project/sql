/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PPLQueryDataAnonymizerTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Test
  public void testSearchCommand() {
    assertEquals("source=t | where a = ***", anonymize("search source=t a=1"));
  }

  @Test
  public void testTableFunctionCommand() {
    assertEquals(
        "source=prometheus.query_range(***,***,***,***)",
        anonymize("source=prometheus.query_range('afsd',123,123,3)"));
  }

  @Test
  public void testPrometheusPPLCommand() {
    assertEquals(
        "source=prometheus.http_requests_process",
        anonymize("source=prometheus.http_requests_process"));
  }

  @Test
  public void testWhereCommand() {
    assertEquals("source=t | where a = ***", anonymize("search source=t | where a=1"));
  }

  @Test
  public void testFieldsCommandWithoutArguments() {
    assertEquals("source=t | fields + f,g", anonymize("source=t | fields f,g"));
  }

  @Test
  public void testFieldsCommandWithIncludeArguments() {
    assertEquals("source=t | fields + f,g", anonymize("source=t | fields + f,g"));
  }

  @Test
  public void testFieldsCommandWithExcludeArguments() {
    assertEquals("source=t | fields - f,g", anonymize("source=t | fields - f,g"));
  }

  @Test
  public void testRenameCommandWithMultiFields() {
    assertEquals(
        "source=t | rename f as g,h as i,j as k",
        anonymize("source=t | rename f as g,h as i,j as k"));
  }

  @Test
  public void testStatsCommandWithByClause() {
    assertEquals("source=t | stats count(a) by b", anonymize("source=t | stats count(a) by b"));
  }

  @Test
  public void testStatsCommandWithNestedFunctions() {
    assertEquals("source=t | stats sum(+(a,b))", anonymize("source=t | stats sum(a+b)"));
  }

  @Test
  public void testDedupCommand() {
    assertEquals(
        "source=t | dedup f1,f2 1 keepempty=false consecutive=false",
        anonymize("source=t | dedup f1, f2"));
  }

  @Test
  public void testTrendlineCommand() {
    assertEquals(
        "source=t | trendline sma(2, date) as date_alias sma(3, time) as time_alias",
        anonymize("source=t | trendline sma(2, date) as date_alias sma(3, time) as time_alias"));
  }

  @Test
  public void testFlattenCommand() {
    assertEquals(
        "Flatten command is not modified by anonymizer",
        "source=t | flatten field_name",
        anonymize("source=t | flatten field_name"));
  }

  @Test
  public void testHeadCommandWithNumber() {
    assertEquals("source=t | head 3", anonymize("source=t | head 3"));
  }

  // todo, sort order is ignored, it doesn't impact the log analysis.
  @Test
  public void testSortCommandWithOptions() {
    assertEquals("source=t | sort f1,f2", anonymize("source=t | sort - f1, + f2"));
  }

  @Test
  public void testEvalCommand() {
    assertEquals("source=t | eval r=abs(f)", anonymize("source=t | eval r=abs(f)"));
  }

  @Test
  public void testFillNullSameValue() {
    assertEquals(
        "source=t | fillnull with 0 in f1, f2", anonymize("source=t | fillnull with 0 in f1, f2"));
  }

  @Test
  public void testFillNullVariousValues() {
    assertEquals(
        "source=t | fillnull using f1 = 0, f2 = -1",
        anonymize("source=t | fillnull using f1 = 0, f2 = -1"));
  }

  @Test
  public void testRareCommandWithGroupBy() {
    assertEquals("source=t | rare 10 a by b", anonymize("source=t | rare a by b"));
  }

  @Test
  public void testTopCommandWithNAndGroupBy() {
    assertEquals("source=t | top 1 a by b", anonymize("source=t | top 1 a by b"));
  }

  @Test
  public void testAndExpression() {
    assertEquals("source=t | where a = *** and b = ***", anonymize("source=t | where a=1 and b=2"));
  }

  @Test
  public void testOrExpression() {
    assertEquals("source=t | where a = *** or b = ***", anonymize("source=t | where a=1 or b=2"));
  }

  @Test
  public void testXorExpression() {
    assertEquals("source=t | where a = *** xor b = ***", anonymize("source=t | where a=1 xor b=2"));
  }

  @Test
  public void testNotExpression() {
    assertEquals("source=t | where not a = ***", anonymize("source=t | where not a=1 "));
  }

  @Test
  public void testQualifiedName() {
    assertEquals("source=t | fields + field0", anonymize("source=t | fields field0"));
  }

  @Test
  public void testDateFunction() {
    assertEquals(
        "source=t | eval date=DATE_ADD(DATE(***),INTERVAL *** HOUR)",
        anonymize("source=t | eval date=DATE_ADD(DATE('2020-08-26'),INTERVAL 1 HOUR)"));
  }

  @Test
  public void testExplain() {
    assertEquals("source=t | fields + a", anonymizeStatement("source=t | fields a", true));
  }

  @Test
  public void testQuery() {
    assertEquals("source=t | fields + a", anonymizeStatement("source=t | fields a", false));
  }

  @Test
  public void anonymizeFieldsNoArg() {
    assertEquals(
        "source=t | fields + f",
        anonymize(projectWithArg(relation("t"), Collections.emptyList(), field("f"))));
  }

  private String anonymize(String query) {
    AstBuilder astBuilder = new AstBuilder(new AstExpressionBuilder(), query);
    return anonymize(astBuilder.visit(parser.parse(query)));
  }

  private String anonymize(UnresolvedPlan plan) {
    final PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer();
    return anonymize.anonymizeData(plan);
  }

  private String anonymizeStatement(String query, boolean isExplain) {
    AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(new AstExpressionBuilder(), query),
            AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
    Statement statement = builder.visit(parser.parse(query));
    PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer();
    return anonymize.anonymizeStatement(statement);
  }
}

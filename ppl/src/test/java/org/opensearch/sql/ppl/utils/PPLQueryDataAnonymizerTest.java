/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PPLQueryDataAnonymizerTest {

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Mock private Settings settings;

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
  public void testStatsCommandWithSpanFunction() {
    assertEquals(
        "source=t | stats count(a) by span(b, *** d),c",
        anonymize("source=t | stats count(a) by span(b, 1d), c"));
  }

  @Test
  public void testEventstatsCommandWithByClause() {
    assertEquals(
        "source=t | eventstats count(a) by b", anonymize("source=t | eventstats count(a) by b"));
  }

  @Test
  public void testEventstatsCommandWithNestedFunctions() {
    assertEquals("source=t | eventstats sum(+(a,b))", anonymize("source=t | eventstats sum(a+b)"));
  }

  @Test
  public void testEventstatsCommandWithSpanFunction() {
    assertEquals(
        "source=t | eventstats count(a) by span(b, *** d),c",
        anonymize("source=t | eventstats count(a) by span(b, 1d), c"));
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
        "source=t | fillnull with *** in f1, f2",
        anonymize("source=t | fillnull with 0 in f1, f2"));
  }

  @Test
  public void testFillNullVariousValues() {
    assertEquals(
        "source=t | fillnull using f1 = ***, f2 = ***",
        anonymize("source=t | fillnull using f1 = 0, f2 = -1"));
  }

  @Test
  public void testFillNullWithoutFields() {
    assertEquals("source=t | fillnull with ***", anonymize("source=t | fillnull with 0"));
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
  public void testInExpression() {
    assertEquals("source=t | where a in (***)", anonymize("source=t | where a in (1, 2, 3) "));
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
  public void testDescribe() {
    assertEquals("describe t", anonymize("describe t"));
  }

  @Test
  public void testExplain() {
    assertEquals(
        "explain standard source=t | fields + a", anonymizeStatement("source=t | fields a", true));
  }

  @Test
  public void testExplainCommand() {
    assertEquals(
        "explain standard source=t | fields + a",
        anonymizeStatement("explain source=t | fields a", false));
    assertEquals(
        "explain extended source=t | fields + a",
        anonymizeStatement("explain extended source=t | fields a", false));
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

  @Test
  public void testBetween() {
    assertEquals(
        "source=t | where id between *** and *** | fields + id",
        anonymize("source=t | where id between 1 and 2 | fields id"));
    assertEquals(
        "source=t | where not id between *** and *** | fields + id",
        anonymize("source=t | where id not between 1 and 2 | fields id"));
  }

  @Test
  public void testSubqueryAlias() {
    assertEquals("source=t as t1", anonymize("source=t as t1"));
  }

  @Test
  public void testJoin() {
    assertEquals(
        "source=t | cross join on true s | fields + id",
        anonymize("source=t | cross join s | fields id"));
    assertEquals(
        "source=t | inner join on id = uid s | fields + id",
        anonymize("source=t | inner join on id = uid s | fields id"));
    assertEquals(
        "source=t as l | inner join left = l right = r on id = uid s as r | fields + id",
        anonymize("source=t | join left = l right = r on id = uid s | fields id"));
    assertEquals(
        "source=t | left join right = r on id = uid s as r | fields + id",
        anonymize("source=t | left join right = r on id = uid s | fields id"));
    assertEquals(
        "source=t as t1 | inner join on id = uid s as t2 | fields + t1.id",
        anonymize("source=t as t1 | inner join on id = uid s as t2 | fields t1.id"));
    assertEquals(
        "source=t as t1 | right join on t1.id = t2.id s as t2 | fields + t1.id",
        anonymize("source=t as t1 | right join on t1.id = t2.id s as t2 | fields t1.id"));
    assertEquals(
        "source=t as t1 | right join right = t2 on t1.id = t2.id [ source=s | fields + id ] as t2 |"
            + " fields + t1.id",
        anonymize(
            "source=t as t1 | right join on t1.id = t2.id [ source=s | fields id] as t2 | fields"
                + " t1.id"));
  }

  @Test
  public void testLookup() {
    assertEquals(
        "source=EMP | lookup DEPT DEPTNO replace LOC",
        anonymize("source=EMP | lookup DEPT DEPTNO replace LOC"));
    assertEquals(
        "source=EMP | lookup DEPT DEPTNO replace LOC as JOB",
        anonymize("source=EMP | lookup DEPT DEPTNO replace LOC as JOB"));
    assertEquals(
        "source=EMP | lookup DEPT DEPTNO append LOC",
        anonymize("source=EMP | lookup DEPT DEPTNO append LOC"));
    assertEquals(
        "source=EMP | lookup DEPT DEPTNO append LOC as JOB",
        anonymize("source=EMP | lookup DEPT DEPTNO append LOC as JOB"));
    assertEquals("source=EMP | lookup DEPT DEPTNO", anonymize("source=EMP | lookup DEPT DEPTNO"));
    assertEquals(
        "source=EMP | lookup DEPT DEPTNO as EMPNO, ID append ID, LOC as JOB, COUNTRY as COUNTRY2",
        anonymize(
            "source=EMP | lookup DEPT DEPTNO as EMPNO, ID append ID, LOC as JOB, COUNTRY as"
                + " COUNTRY2"));
  }

  @Test
  public void testInSubquery() {
    assertEquals(
        "source=t | where (id) in [ source=s | fields + uid ] | fields + id",
        anonymize("source=t | where id in [source=s | fields uid] | fields id"));
  }

  @Test
  public void testExistsSubquery() {
    assertEquals(
        "source=t | where exists [ source=s | where id = uid ] | fields + id",
        anonymize("source=t | where exists [source=s | where id = uid ] | fields id"));
  }

  @Test
  public void testScalarSubquery() {
    assertEquals(
        "source=t | where id = [ source=s | stats max(b) ] | fields + id",
        anonymize("source=t |  where id = [ source=s | stats max(b) ] | fields id"));
    assertEquals(
        "source=t | eval id=[ source=s | stats max(b) ] | fields + id",
        anonymize("source=t |  eval id = [ source=s | stats max(b) ] | fields id"));
    assertEquals(
        "source=t | where id > [ source=s | where id = uid | stats max(b) ] | fields + id",
        anonymize("source=t id > [ source=s | where id = uid | stats max(b) ] | fields id"));
  }

  @Test
  public void testCaseWhen() {
    assertEquals(
        "source=t | eval level=cast(score >= ***,***,score >= *** and score < ***,*** else ***) |"
            + " fields + level",
        anonymize(
            "source=t | eval level=CASE(score >= 90, 'A', score >= 80 AND score < 90, 'B' else 'C')"
                + " | fields level"));
    assertEquals(
        "source=t | eval level=cast(score >= ***,***,score >= *** and score < ***,***) | fields +"
            + " level",
        anonymize(
            "source=t | eval level=CASE(score >= 90, 'A', score >= 80 AND score < 90, 'B')"
                + " | fields level"));
  }

  @Test
  public void testCast() {
    assertEquals(
        "source=t | eval id=cast(a as INTEGER) | fields + id",
        anonymize("source=t | eval id=CAST(a AS INTEGER) | fields id"));
    assertEquals(
        "source=t | eval id=cast(*** as DOUBLE) | fields + id",
        anonymize("source=t | eval id=CAST('1' AS DOUBLE) | fields id"));
  }

  @Test
  public void testParse() {
    assertEquals(
        "source=t | parse email '.+@(?<email>.+)'",
        anonymize("source=t | parse email '.+@(?<email>.+)'"));
    assertEquals(
        "source=t | parse email '.+@(?<host>.+)' | fields + email,host",
        anonymize("source=t | parse email '.+@(?<host>.+)' | fields email, host"));
  }

  @Test
  public void testPatterns() {
    when(settings.getSettingValue(Key.DEFAULT_PATTERN_METHOD)).thenReturn("SIMPLE_PATTERN");
    assertEquals("source=t | patterns email", anonymize("source=t | patterns email"));
    assertEquals(
        "source=t | patterns email | fields + email,patterns_field",
        anonymize("source=t | patterns email | fields email, patterns_field"));
  }

  private String anonymize(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return anonymize(astBuilder.visit(parser.parse(query)));
  }

  private String anonymize(UnresolvedPlan plan) {
    final PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer();
    return anonymize.anonymizeData(plan);
  }

  private String anonymizeStatement(String query, boolean isExplain) {
    AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
    Statement statement = builder.visit(parser.parse(query));
    PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer();
    return anonymize.anonymizeStatement(statement);
  }
}

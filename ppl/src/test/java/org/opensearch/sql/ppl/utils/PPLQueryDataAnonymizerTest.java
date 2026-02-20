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
    assertEquals("source=table identifier = ***", anonymize("search source=t a=1"));
  }

  @Test
  public void testTableFunctionCommand() {
    assertEquals(
        "source=prometheus.query_range(***,***,***,***)",
        anonymize("source=prometheus.query_range('afsd',123,123,3)"));
  }

  @Test
  public void testPrometheusPPLCommand() {
    assertEquals("source=table", anonymize("source=prometheus.http_requests_process"));
  }

  @Test
  public void testWhereCommand() {
    assertEquals("source=table | where identifier = ***", anonymize("search source=t | where a=1"));
  }

  @Test
  public void testLikeFunction() {
    assertEquals(
        "source=table | where like(identifier,***)",
        anonymize("search source=t | where like(a, '%llo%')"));
    assertEquals(
        "source=table | where like(identifier,***,***)",
        anonymize("search source=t | where like(a, '%llo%', true)"));
    assertEquals(
        "source=table | where like(identifier,***,***)",
        anonymize("search source=t | where like(a, '%llo%', false)"));
  }

  // Fields and Table Command Tests
  @Test
  public void testFieldsCommandWithoutArguments() {
    assertEquals(
        "source=table | fields + identifier,identifier", anonymize("source=t | fields f,g"));
  }

  @Test
  public void testFieldsCommandWithIncludeArguments() {
    assertEquals(
        "source=table | fields + identifier,identifier", anonymize("source=t | fields + f,g"));
  }

  @Test
  public void testFieldsCommandWithExcludeArguments() {
    assertEquals(
        "source=table | fields - identifier,identifier", anonymize("source=t | fields - f,g"));
  }

  @Test
  public void testFieldsCommandWithWildcards() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    assertEquals("source=table | fields + identifier", anonymize("source=t | fields account*"));
    assertEquals("source=table | fields + identifier", anonymize("source=t | fields *name"));
    assertEquals("source=table | fields + identifier", anonymize("source=t | fields *a*"));
    assertEquals("source=table | fields - identifier", anonymize("source=t | fields - account*"));
  }

  @Test
  public void testFieldsCommandWithDelimiters() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    assertEquals(
        "source=table | fields + identifier,identifier,identifier",
        anonymize("source=t | fields firstname lastname age"));
    assertEquals(
        "source=table | fields + identifier,identifier,identifier",
        anonymize("source=t | fields firstname lastname, balance"));
    assertEquals(
        "source=table | fields + identifier,identifier",
        anonymize("source=t | fields account*, *name"));
  }

  @Test
  public void testTableCommand() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    assertEquals(
        "source=table | fields + identifier,identifier", anonymize("source=t | table f,g"));
    assertEquals(
        "source=table | fields + identifier,identifier", anonymize("source=t | table + f,g"));
    assertEquals(
        "source=table | fields - identifier,identifier", anonymize("source=t | table - f,g"));
    assertEquals("source=table | fields + identifier", anonymize("source=t | table account*"));
    assertEquals(
        "source=table | fields + identifier,identifier,identifier",
        anonymize("source=t | table firstname lastname age"));
  }

  @Test
  public void anonymizeFieldsNoArg() {
    assertEquals(
        "source=table | fields + identifier",
        anonymize(projectWithArg(relation("t"), Collections.emptyList(), field("f"))));
  }

  @Test
  public void testRenameCommandWithMultiFields() {
    assertEquals(
        "source=table | rename identifier as identifier,identifier as identifier,identifier as"
            + " identifier",
        anonymize("source=t | rename f as g,h as i,j as k"));
  }

  @Test
  public void testRenameCommandWithWildcards() {
    assertEquals(
        "source=table | rename identifier as identifier", anonymize("source=t | rename f* as g*"));
  }

  @Test
  public void testStatsCommandWithByClause() {
    assertEquals(
        "source=table | stats count(identifier) by identifier",
        anonymize("source=t | stats count(a) by b"));
  }

  @Test
  public void testStatsCommandWithNestedFunctions() {
    assertEquals(
        "source=table | stats sum(+(identifier,identifier))",
        anonymize("source=t | stats sum(a+b)"));
  }

  @Test
  public void testStatsCommandWithSpanFunction() {
    assertEquals(
        "source=table | stats count(identifier) by span(identifier, *** d),identifier",
        anonymize("source=t | stats count(a) by span(b, 1d), c"));
  }

  @Test
  public void testEventstatsCommandWithByClause() {
    assertEquals(
        "source=table | eventstats count(identifier) by identifier",
        anonymize("source=t | eventstats count(a) by b"));
  }

  @Test
  public void testEventstatsCommandWithNestedFunctions() {
    assertEquals(
        "source=table | eventstats sum(+(identifier,identifier))",
        anonymize("source=t | eventstats sum(a+b)"));
  }

  @Test
  public void testEventstatsCommandWithSpanFunction() {
    assertEquals(
        "source=table | eventstats count(identifier) by span(identifier, *** d),identifier",
        anonymize("source=t | eventstats count(a) by span(b, 1d), c"));
  }

  @Test
  public void testStreamstatsCommandWithByClause() {
    assertEquals(
        "source=table | streamstats count(identifier) by identifier",
        anonymize("source=t | streamstats count(a) by b"));
  }

  @Test
  public void testStreamstatsCommandWithWindowAndCurrent() {
    assertEquals(
        "source=table | streamstats max(identifier)",
        anonymize("source=t | streamstats current=false window=2 max(a)"));
  }

  @Test
  public void testStreamstatsCommandWithNestedFunctions() {
    assertEquals(
        "source=table | streamstats sum(+(identifier,identifier))",
        anonymize("source=t | streamstats sum(a+b)"));
  }

  @Test
  public void testStreamstatsCommandWithSpanFunction() {
    assertEquals(
        "source=table | streamstats count(identifier) by span(identifier, *** d),identifier",
        anonymize("source=t | streamstats count(a) by span(b, 1d), c"));
  }

  @Test
  public void testBinCommandBasic() {
    assertEquals("source=table | bin identifier span=***", anonymize("source=t | bin f span=10"));
  }

  @Test
  public void testBinCommandWithAllParameters() {
    assertEquals(
        "source=table | bin identifier span=*** aligntime=*** as identifier",
        anonymize("source=t | bin f span=10 aligntime=earliest as alias"));
  }

  @Test
  public void testBinCommandWithCountParameters() {
    assertEquals(
        "source=table | bin identifier bins=*** start=*** end=*** as identifier",
        anonymize("source=t | bin f bins=10 start=0 end=100 as alias"));
  }

  @Test
  public void testBinCommandWithMinspanParameters() {
    assertEquals(
        "source=table | bin identifier minspan=*** start=*** end=*** as identifier",
        anonymize("source=t | bin f minspan=5 start=0 end=100 as alias"));
  }

  @Test
  public void testDedupCommand() {
    assertEquals(
        "source=table | dedup identifier,identifier 1 keepempty=false consecutive=false",
        anonymize("source=t | dedup f1, f2"));
  }

  @Test
  public void testTransposeCommand() {
    assertEquals(
        "source=table | transpose 5 column_name=***",
        anonymize("source=t | transpose 5 column_name='column_names'"));
  }

  @Test
  public void testTrendlineCommand() {
    assertEquals(
        "source=table | trendline sma(2, identifier) as identifier sma(3, identifier) as"
            + " identifier",
        anonymize("source=t | trendline sma(2, date) as date_alias sma(3, time) as time_alias"));
  }

  @Test
  public void testHeadCommandWithNumber() {
    assertEquals("source=table | head 3", anonymize("source=t | head 3"));
  }

  @Test
  public void testReverseCommand() {
    assertEquals("source=table | reverse", anonymize("source=t | reverse"));
  }

  @Test
  public void testTimechartCommand() {
    assertEquals(
        "source=table | timechart count() by identifier",
        anonymize("source=t | timechart count() by host"));

    assertEquals(
        "source=table | timechart timefield=time_identifier max(identifier)",
        anonymize("source=t | timechart timefield=month max(revenue)"));
  }

  @Test
  public void testChartCommand() {
    assertEquals(
        "source=table | chart count(identifier) by identifier identifier",
        anonymize("source=t | chart count(age) by gender country"));
  }

  @Test
  public void testChartCommandWithParameters() {
    assertEquals(
        "source=table | chart limit=*** useother=*** avg(identifier) by identifier",
        anonymize("source=t | chart limit=5 useother=false avg(balance) by state"));
  }

  @Test
  public void testChartCommandOver() {
    assertEquals(
        "source=table | chart avg(identifier) by identifier",
        anonymize("source=t | chart avg(balance) over gender"));
  }

  @Test
  public void testChartCommandOverBy() {
    assertEquals(
        "source=table | chart sum(identifier) by identifier identifier",
        anonymize("source=t | chart sum(amount) over gender by age"));
  }

  // todo, sort order is ignored, it doesn't impact the log analysis.
  @Test
  public void testSortCommandWithOptions() {
    assertEquals(
        "source=table | sort identifier,identifier", anonymize("source=t | sort - f1, + f2"));
  }

  @Test
  public void testSortCommandWithCount() {
    assertEquals("source=table | sort 5 identifier", anonymize("source=t | sort 5 f1"));
  }

  @Test
  public void testSortCommandWithDesc() {
    assertEquals("source=table | sort identifier", anonymize("source=t | sort f1 desc"));
  }

  @Test
  public void testEvalCommand() {
    assertEquals(
        "source=table | eval identifier=abs(identifier)", anonymize("source=t | eval r=abs(f)"));
  }

  @Test
  public void testEvalCommandWithStrftime() {
    assertEquals(
        "source=table | eval identifier=strftime(identifier,***)",
        anonymize("source=t | eval formatted=strftime(timestamp, '%Y-%m-%d %H:%M:%S')"));
  }

  @Test
  public void testFillNullSameValue() {
    assertEquals(
        "source=table | fillnull with *** in identifier, identifier",
        anonymize("source=t | fillnull with 0 in f1, f2"));
  }

  @Test
  public void testFillNullVariousValues() {
    assertEquals(
        "source=table | fillnull using identifier = ***, identifier = ***",
        anonymize("source=t | fillnull using f1 = 0, f2 = -1"));
  }

  @Test
  public void testFillNullWithoutFields() {
    assertEquals("source=table | fillnull with ***", anonymize("source=t | fillnull with 0"));
  }

  @Test
  public void testFillNullValueSyntaxWithFields() {
    assertEquals(
        "source=table | fillnull value=*** identifier identifier",
        anonymize("source=t | fillnull value=0 f1 f2"));
  }

  @Test
  public void testFillNullValueSyntaxAllFields() {
    assertEquals("source=table | fillnull value=***", anonymize("source=t | fillnull value=0"));
  }

  @Test
  public void testRareCommandWithGroupBy() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(false);
    assertEquals(
        "source=table | rare 10 identifier by identifier", anonymize("source=t | rare a by b"));
  }

  @Test
  public void testTopCommandWithNAndGroupBy() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(false);
    assertEquals(
        "source=table | top 1 identifier by identifier", anonymize("source=t | top 1 a by b"));
  }

  @Test
  public void testRareCommandWithGroupByWithCalcite() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    assertEquals(
        "source=table | rare 10 countield='count' showcount=true usenull=true identifier by"
            + " identifier",
        anonymize("source=t | rare a by b"));
  }

  @Test
  public void testTopCommandWithNAndGroupByWithCalcite() {
    when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    assertEquals(
        "source=table | top 1 countield='count' showcount=true usenull=true identifier by"
            + " identifier",
        anonymize("source=t | top 1 a by b"));
  }

  @Test
  public void testAndExpression() {
    assertEquals(
        "source=table | where identifier = *** and identifier = ***",
        anonymize("source=t | where a=1 and b=2"));
  }

  @Test
  public void testAndExpressionWithMetaData() {
    assertEquals(
        "source=table | where meta_identifier = *** and identifier = ***",
        anonymize("source=t | where _id=1 and b=2"));
  }

  @Test
  public void testOrExpression() {
    assertEquals(
        "source=table | where identifier = *** or identifier = ***",
        anonymize("source=t | where a=1 or b=2"));
  }

  @Test
  public void testXorExpression() {
    assertEquals(
        "source=table | where identifier = *** xor identifier = ***",
        anonymize("source=t | where a=1 xor b=2"));
  }

  @Test
  public void testNotExpression() {
    assertEquals(
        "source=table | where not identifier = ***", anonymize("source=t | where not a=1 "));
  }

  @Test
  public void testInExpression() {
    assertEquals(
        "source=table | where identifier in (***)", anonymize("source=t | where a in (1, 2, 3) "));
  }

  @Test
  public void testQualifiedName() {
    assertEquals("source=table | fields + identifier", anonymize("source=t | fields field0"));
  }

  @Test
  public void testDateFunction() {
    assertEquals(
        "source=table | eval identifier=DATE_ADD(DATE(***),INTERVAL *** HOUR)",
        anonymize("source=t | eval date=DATE_ADD(DATE('2020-08-26'),INTERVAL 1 HOUR)"));
  }

  @Test
  public void testDescribe() {
    assertEquals("describe table", anonymize("describe t"));
  }

  @Test
  public void testExplain() {
    assertEquals(
        "explain standard source=table | fields + identifier",
        anonymizeStatement("source=t | fields a", true));
  }

  @Test
  public void testExplainCommand() {
    assertEquals(
        "explain standard source=table | fields + identifier",
        anonymizeStatement("explain source=t | fields a", false));
    assertEquals(
        "explain extended source=table | fields + identifier",
        anonymizeStatement("explain extended source=t | fields a", false));
  }

  @Test
  public void testQuery() {
    assertEquals(
        "source=table | fields + identifier", anonymizeStatement("source=t | fields a", false));
  }

  @Test
  public void testBetween() {
    assertEquals(
        "source=table | where identifier between *** and *** | fields + identifier",
        anonymize("source=t | where id between 1 and 2 | fields id"));
    assertEquals(
        "source=table | where not identifier between *** and *** | fields + identifier",
        anonymize("source=t | where id not between 1 and 2 | fields id"));
  }

  @Test
  public void testAppendcol() {
    assertEquals(
        "source=table | stats count() by identifier | appendcol override=false [ stats"
            + " sum(identifier) by identifier ]",
        anonymize("source=t | stats count() by b | appendcol [ stats sum(c) by b ]"));
    assertEquals(
        "source=table | stats count() by identifier | appendcol override=true [ stats"
            + " sum(identifier) by identifier ]",
        anonymize("source=t | stats count() by b | appendcol override=true [ stats sum(c) by b ]"));
    assertEquals(
        "source=table | appendcol override=false [ where identifier = *** ]",
        anonymize("source=t | appendcol override=false [ where a = 1 ]"));
  }

  @Test
  public void testAddTotals() {
    assertEquals(
        "source=table | addtotals row=true col=true label=identifier labelfield=identifier"
            + " fieldname=identifier",
        anonymize(
            "source=table | addtotals row=true col=true label='identifier' labelfield='identifier'"
                + " fieldname='identifier'"));
  }

  @Test
  public void testAddColTotals() {
    assertEquals(
        "source=table | addcoltotals label=identifier labelfield=identifier",
        anonymize("source=table | addcoltotals label='identifier' labelfield='identifier'"));
  }

  @Test
  public void testAppend() {
    assertEquals(
        "source=table | stats count() by identifier | append [ | stats sum(identifier) by"
            + " identifier ]",
        anonymize("source=t | stats count() by b | append [ | stats sum(c) by b ]"));
    assertEquals(
        "source=table | stats count() by identifier | append [ | stats sum(identifier) by"
            + " identifier ]",
        anonymize("source=t | stats count() by b | append [ | stats sum(c) by b ]"));
    assertEquals(
        "source=table | append [ | where identifier = *** ]",
        anonymize("source=t | append [ | where a = 1 ]"));
    assertEquals(
        "source=table | stats count() by identifier | append [source=table | stats sum(identifier)"
            + " by identifier ]",
        anonymize("source=t | stats count() by b | append [source=a | stats sum(c) by b ]"));
    assertEquals(
        "source=table | append [source=table | where identifier = *** ]",
        anonymize("source=t | append [source=b | where a = 1 ]"));
    assertEquals(
        "source=table | stats count() by identifier | append [source=table ]",
        anonymize("source=t | stats count() by b | append [ source=a ]"));
    assertEquals(
        "source=table | stats count() by identifier | append [ ]",
        anonymize("source=t | stats count() by b | append [ ]"));
  }

  @Test
  // Same as SQL, select * from a as b -> SELECT * FROM table AS identifier
  public void testSubqueryAlias() {
    assertEquals("source=table as identifier", anonymize("source=t as t1"));
  }

  @Test
  public void testJoin() {
    assertEquals(
        "source=table | cross join max=*** on *** = *** table | fields + identifier",
        anonymize("source=t | cross join on 1=1 s | fields id"));
    assertEquals(
        "source=table | inner join max=*** on identifier = identifier table | fields + identifier",
        anonymize("source=t | inner join on id = uid s | fields id"));
    assertEquals(
        "source=table as identifier | inner join max=*** left = identifier right = identifier on"
            + " identifier = identifier table as identifier | fields + identifier",
        anonymize("source=t | join left = l right = r on id = uid s | fields id"));
    assertEquals(
        "source=table | left join max=*** right = identifier on identifier = identifier table as"
            + " identifier | fields + identifier",
        anonymize("source=t | left join right = r on id = uid s | fields id"));
    assertEquals(
        "source=table as identifier | inner join max=*** left = identifier right = identifier on"
            + " identifier = identifier table as identifier | fields + identifier",
        anonymize("source=t as t1 | inner join on id = uid s as t2 | fields t1.id"));
    assertEquals(
        "source=table as identifier | right join max=*** left = identifier right = identifier on"
            + " identifier = identifier table as identifier | fields + identifier",
        anonymize("source=t as t1 | right join max=0 on t1.id = t2.id s as t2 | fields t1.id"));
    assertEquals(
        "source=table as identifier | right join max=*** left = identifier right = identifier on"
            + " identifier = identifier [ source=table | fields + identifier ] as identifier |"
            + " fields + identifier",
        anonymize(
            "source=t as t1 | right join max=0 on t1.id = t2.id [ source=s | fields id] as t2 |"
                + " fields t1.id"));
    assertEquals(
        "source=table | inner join max=*** on identifier = identifier table | fields + identifier",
        anonymize("source=t | inner join max=2 on id = uid s | fields id"));
  }

  @Test
  public void testJoinWithFieldList() {
    assertEquals(
        "source=table | join type=inner overwrite=*** max=***  table | fields + identifier",
        anonymize("source=t | join s | fields id"));
    assertEquals(
        "source=table | join type=inner overwrite=*** max=*** identifier table | fields +"
            + " identifier",
        anonymize("source=t | join id s | fields id"));
    assertEquals(
        "source=table | join type=left overwrite=*** max=*** identifier,identifier table | fields +"
            + " identifier",
        anonymize("source=t | join type=left overwrite=false id1,id2 s | fields id1"));
    assertEquals(
        "source=table | join type=left overwrite=*** max=*** identifier,identifier table | fields +"
            + " identifier",
        anonymize("source=t | join type=outer overwrite=false id1 id2 s | fields id1"));
    assertEquals(
        "source=table | join type=left overwrite=*** max=*** identifier,identifier table | fields +"
            + " identifier",
        anonymize("source=t | join type=outer max=2 id1 id2 s | fields id1"));
  }

  @Test
  public void testLookup() {
    assertEquals(
        "source=table | lookup table DEPTNO replace LOC",
        anonymize("source=EMP | lookup DEPT DEPTNO replace LOC"));
    assertEquals(
        "source=table | lookup table DEPTNO replace LOC as JOB",
        anonymize("source=EMP | lookup DEPT DEPTNO replace LOC as JOB"));
    assertEquals(
        "source=table | lookup table DEPTNO append LOC",
        anonymize("source=EMP | lookup DEPT DEPTNO append LOC"));
    assertEquals(
        "source=table | lookup table DEPTNO append LOC as JOB",
        anonymize("source=EMP | lookup DEPT DEPTNO append LOC as JOB"));
    assertEquals(
        "source=table | lookup table DEPTNO", anonymize("source=EMP | lookup DEPT DEPTNO"));
    assertEquals(
        "source=table | lookup table DEPTNO as EMPNO, ID append ID, LOC as JOB, COUNTRY as"
            + " COUNTRY2",
        anonymize(
            "source=EMP | lookup DEPT DEPTNO as EMPNO, ID append ID, LOC as JOB, COUNTRY as"
                + " COUNTRY2"));
  }

  @Test
  public void testInSubquery() {
    assertEquals(
        "source=table | where (identifier) in [ source=table | fields + identifier ] | fields +"
            + " identifier",
        anonymize("source=t | where id in [source=s | fields uid] | fields id"));
  }

  @Test
  public void testExistsSubquery() {
    assertEquals(
        "source=table | where exists [ source=table | where identifier = identifier ] | fields +"
            + " identifier",
        anonymize("source=t | where exists [source=s | where id = uid ] | fields id"));
  }

  @Test
  public void testScalarSubquery() {
    assertEquals(
        "source=table | where identifier = [ source=table | stats max(identifier) ] | fields +"
            + " identifier",
        anonymize("source=t |  where id = [ source=s | stats max(b) ] | fields id"));
    assertEquals(
        "source=table | eval identifier=[ source=table | stats max(identifier) ] | fields +"
            + " identifier",
        anonymize("source=t |  eval id = [ source=s | stats max(b) ] | fields id"));
    assertEquals(
        "source=table | where identifier > [ source=table | where identifier = identifier | stats"
            + " max(identifier) ] | fields + identifier",
        anonymize(
            "source=t | where id > [ source=s | where id = uid | stats max(b) ] | fields id"));
  }

  @Test
  public void testCaseWhen() {
    assertEquals(
        "source=table | eval identifier=case(identifier >= ***,***,identifier >= *** and identifier"
            + " < ***,*** else ***) | fields + identifier",
        anonymize(
            "source=t | eval level=CASE(score >= 90, 'A', score >= 80 AND score < 90, 'B' else 'C')"
                + " | fields level"));
    assertEquals(
        "source=table | eval identifier=case(identifier >= ***,***,identifier >= *** and identifier"
            + " < ***,***) | fields + identifier",
        anonymize(
            "source=t | eval level=CASE(score >= 90, 'A', score >= 80 AND score < 90, 'B')"
                + " | fields level"));
  }

  @Test
  public void testCast() {
    assertEquals(
        "source=table | eval identifier=cast(identifier as INTEGER) | fields + identifier",
        anonymize("source=t | eval id=CAST(a AS INTEGER) | fields id"));
    assertEquals(
        "source=table | eval identifier=cast(*** as DOUBLE) | fields + identifier",
        anonymize("source=t | eval id=CAST('1' AS DOUBLE) | fields id"));
  }

  @Test
  public void testParse() {
    assertEquals(
        "source=table | parse identifier '***'",
        anonymize("source=t | parse email '.+@(?<email>.+)'"));
    assertEquals(
        "source=table | parse identifier '***' | fields + identifier,identifier",
        anonymize("source=t | parse email '.+@(?<host>.+)' | fields email, host"));
  }

  @Test
  public void testGrok() {
    assertEquals(
        "source=table | grok identifier '***'",
        anonymize("source=t | grok email '.+@%{HOSTNAME:host}'"));
    assertEquals(
        "source=table | grok identifier '***' | fields + identifier,identifier",
        anonymize("source=t | grok email '.+@%{HOSTNAME:host}' | fields email, host"));
  }

  @Test
  public void testReplaceCommandSingleField() {
    assertEquals(
        "source=table | replace *** WITH *** IN Field(field=fieldname, fieldArgs=[])",
        anonymize("source=EMP | replace \"value\" WITH \"newvalue\" IN fieldname"));
  }

  @Test
  public void testReplaceCommandMultipleFields() {
    assertEquals(
        "source=table | replace *** WITH *** IN Field(field=fieldname1, fieldArgs=[]),"
            + " Field(field=fieldname2, fieldArgs=[])",
        anonymize("source=EMP | replace \"value\" WITH \"newvalue\" IN fieldname1, fieldname2"));
  }

  @Test(expected = Exception.class)
  public void testReplaceCommandWithoutInShouldFail() {
    anonymize("source=EMP | replace \"value\" WITH \"newvalue\"");
  }

  @Test
  public void testReplaceCommandSpecialCharactersInFields() {
    assertEquals(
        "source=table | replace *** WITH *** IN Field(field=user.name, fieldArgs=[]),"
            + " Field(field=user.email, fieldArgs=[])",
        anonymize("source=EMP | replace \"value\" WITH \"newvalue\" IN user.name, user.email"));
  }

  @Test
  public void testReplaceCommandWithWildcards() {
    assertEquals(
        "source=table | replace *** WITH *** IN Field(field=fieldname, fieldArgs=[])",
        anonymize("source=EMP | replace \"CLERK*\" WITH \"EMPLOYEE*\" IN fieldname"));
  }

  @Test
  public void testReplaceCommandWithMultipleWildcards() {
    assertEquals(
        "source=table | replace *** WITH *** IN Field(field=fieldname1, fieldArgs=[]),"
            + " Field(field=fieldname2, fieldArgs=[])",
        anonymize("source=EMP | replace \"*TEST*\" WITH \"*NEW*\" IN fieldname1, fieldname2"));
  }

  @Test
  public void testPatterns() {
    when(settings.getSettingValue(Key.PATTERN_METHOD)).thenReturn("SIMPLE_PATTERN");
    when(settings.getSettingValue(Key.PATTERN_MODE)).thenReturn("LABEL");
    when(settings.getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT)).thenReturn(10);
    when(settings.getSettingValue(Key.PATTERN_BUFFER_LIMIT)).thenReturn(100000);
    assertEquals(
        "source=table | patterns identifier method=SIMPLE_PATTERN mode=LABEL"
            + " max_sample_count=*** buffer_limit=*** new_field=identifier",
        anonymize("source=t | patterns email"));
    assertEquals(
        "source=table | patterns identifier method=SIMPLE_PATTERN mode=LABEL"
            + " max_sample_count=*** buffer_limit=*** new_field=identifier |"
            + " fields + identifier,identifier",
        anonymize("source=t | patterns email | fields email, identifier"));
    assertEquals(
        "source=table | patterns identifier method=BRAIN mode=AGGREGATION"
            + " max_sample_count=*** buffer_limit=*** new_field=identifier"
            + " variable_count_threshold=***",
        anonymize(
            "source=t | patterns email method=BRAIN mode=AGGREGATION"
                + " variable_count_threshold=5"));
  }

  @Test
  public void testRegex() {
    assertEquals(
        "source=table | regex identifier=***", anonymize("source=t | regex fieldname='pattern'"));
    assertEquals(
        "source=table | regex identifier!=***", anonymize("source=t | regex fieldname!='pattern'"));
    assertEquals(
        "source=table | regex identifier=*** | fields + identifier",
        anonymize("source=t | regex email='.*@domain.com' | fields email"));
  }

  @Test
  public void testAppendPipe() {
    assertEquals(
        "source=table | appendpipe [ | stats count()]",
        anonymize("source=t | appendpipe [stats count()]"));
    assertEquals(
        "source=table | appendpipe [ | where identifier = ***]",
        anonymize("source=t | appendpipe [where fieldname=='pattern']"));
    assertEquals(
        "source=table | appendpipe [ | sort identifier]",
        anonymize("source=t | appendpipe [sort fieldname]"));
  }

  @Test
  public void testRexCommand() {
    when(settings.getSettingValue(Key.PPL_REX_MAX_MATCH_LIMIT)).thenReturn(10);

    assertEquals(
        "source=table | rex field=identifier mode=extract \"***\" max_match=***",
        anonymize("source=t | rex field=message \"(?<user>[A-Z]+)\""));
    assertEquals(
        "source=table | rex field=identifier mode=extract \"***\" max_match=*** | fields +"
            + " identifier,identifier",
        anonymize("source=table | rex field=identifier \"***\" | fields identifier, identifier"));
    assertEquals(
        "source=table | rex field=identifier mode=extract \"***\" max_match=***",
        anonymize("source=t | rex field=name \"(?<first>[A-Z])\" max_match=3"));
  }

  @Test
  public void testRexSedMode() {
    when(settings.getSettingValue(Key.PPL_REX_MAX_MATCH_LIMIT)).thenReturn(10);

    assertEquals(
        "source=table | rex field=identifier mode=sed \"***\" max_match=***",
        anonymize("source=t | rex field=lastname mode=sed \"s/^[A-Z]/X/\""));
    assertEquals(
        "source=table | rex field=identifier mode=sed \"***\" max_match=*** | fields + identifier",
        anonymize("source=t | rex field=data mode=sed \"s/sensitive/clean/g\" | fields data"));
  }

  @Test
  public void testMvjoin() {
    // Test mvjoin with array of strings
    assertEquals(
        "source=table | eval identifier=mvjoin(array(***,***,***),***) | fields + identifier",
        anonymize("source=t | eval result=mvjoin(array('a', 'b', 'c'), ',') | fields result"));
  }

  @Test
  public void testMvappend() {
    assertEquals(
        "source=table | eval identifier=mvappend(identifier,***,***) | fields + identifier",
        anonymize("source=t | eval result=mvappend(a, 'b', 'c') | fields result"));
  }

  @Test
  public void testMvindex() {
    // Test mvindex with single element access
    assertEquals(
        "source=table | eval identifier=mvindex(array(***,***,***),***) | fields + identifier",
        anonymize("source=t | eval result=mvindex(array('a', 'b', 'c'), 1) | fields result"));
    // Test mvindex with range access
    assertEquals(
        "source=table | eval identifier=mvindex(array(***,***,***,***,***),***,***) | fields +"
            + " identifier",
        anonymize("source=t | eval result=mvindex(array(1, 2, 3, 4, 5), 1, 3) | fields result"));
  }

  @Test
  public void testMvzip() {
    // Test mvzip with custom delimiter
    assertEquals(
        "source=table | eval identifier=mvzip(array(***,***),array(***,***),***) | fields +"
            + " identifier",
        anonymize(
            "source=t | eval result=mvzip(array('a', 'b'), array('x', 'y'), '|') | fields result"));
  }

  @Test
  public void testSplit() {
    // Test split with delimiter
    assertEquals(
        "source=table | eval identifier=split(***,***) | fields + identifier",
        anonymize("source=t | eval result=split('a;b;c', ';') | fields result"));
    // Test split with field reference
    assertEquals(
        "source=table | eval identifier=split(identifier,***) | fields + identifier",
        anonymize("source=t | eval result=split(text, ',') | fields result"));
    // Test split with empty delimiter (splits into characters)
    assertEquals(
        "source=table | eval identifier=split(***,***) | fields + identifier",
        anonymize("source=t | eval result=split('abcd', '') | fields result"));
  }

  @Test
  public void testMvdedup() {
    // Test mvdedup with array containing duplicates
    assertEquals(
        "source=table | eval identifier=mvdedup(array(***,***,***,***,***,***)) | fields +"
            + " identifier",
        anonymize("source=t | eval result=mvdedup(array(1, 2, 2, 3, 1, 4)) | fields result"));
  }

  @Test
  public void testMvmap() {
    assertEquals(
        "source=table | eval identifier=mvmap(identifier,*(identifier,***)) | fields +"
            + " identifier",
        anonymize("source=t | eval result=mvmap(arr, arr * 10) | fields result"));
  }

  @Test
  public void testRexWithOffsetField() {
    when(settings.getSettingValue(Key.PPL_REX_MAX_MATCH_LIMIT)).thenReturn(10);

    assertEquals(
        "source=table | rex field=identifier mode=extract \"***\" max_match=***"
            + " offset_field=identifier",
        anonymize("source=t | rex field=message \"(?<word>[a-z]+)\" offset_field=pos"));
  }

  @Test
  public void testMultisearch() {
    assertEquals(
        "| multisearch [search source=table | where identifier < ***] [search"
            + " source=table | where identifier >= ***]",
        anonymize(
            "| multisearch [search source=accounts | where age < 30] [search"
                + " source=accounts | where age >= 30]"));

    assertEquals(
        "| multisearch [search source=table | where identifier > ***] [search"
            + " source=table | where identifier = ***]",
        anonymize(
            "| multisearch [search source=accounts | where balance > 20000]"
                + " [search source=accounts | where state = 'CA']"));

    assertEquals(
        "| multisearch [search source=table | fields + identifier,identifier] [search"
            + " source=table | where identifier = ***]",
        anonymize(
            "| multisearch [search source=accounts | fields firstname, lastname]"
                + " [search source=accounts | where age = 25]"));
  }

  private String anonymize(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return anonymize(astBuilder.visit(parser.parse(query)));
  }

  private String anonymize(UnresolvedPlan plan) {
    final PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer(settings);
    return anonymize.anonymizeData(plan);
  }

  private String anonymizeStatement(String query, boolean isExplain) {
    AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
    Statement statement = builder.visit(parser.parse(query));
    PPLQueryDataAnonymizer anonymize = new PPLQueryDataAnonymizer(settings);
    return anonymize.anonymizeStatement(statement);
  }

  @Test
  public void testSearchWithAbsoluteTimeRange() {
    assertEquals(
        "source=table (time_identifier >= ***) AND (time_identifier <= ***)",
        anonymize("search source=t earliest='2012-12-10 15:00:00' latest=now"));
  }

  @Test
  public void testSearchWithIn() {
    assertEquals("source=table identifier IN ***", anonymize("search source=t balance in (2000)"));
  }

  @Test
  public void testSearchWithNot() {
    assertEquals(
        "source=table NOT(identifier = ***)", anonymize("search NOT balance=2000 source=t"));
  }

  @Test
  public void testSearchWithGroup() {
    assertEquals(
        "source=table ((identifier = *** OR identifier = ***) AND identifier > ***)",
        anonymize(
            "search (severityText=\"ERROR\" OR severityText=\"WARN\") AND severityNumber>10"
                + " source=t"));
  }

  @Test
  public void testSearchWithOr() {
    assertEquals(
        "source=table (time_identifier >= *** OR time_identifier <= ***)",
        anonymize("search source=t earliest='2012-12-10 15:00:00' or latest=now"));
  }

  @Test
  public void testSpath() {
    assertEquals(
        "source=table | spath input=identifier output=identifier path=identifier | fields +"
            + " identifier,identifier",
        anonymize(
            "search source=t | spath input=json_attr output=out path=foo.bar | fields id, out"));
  }

  @Test
  public void testSpathNoPath() {
    assertEquals(
        "source=table | spath input=identifier",
        anonymize("search source=t | spath input=json_attr"));
  }

  @Test
  public void testMvfind() {
    assertEquals(
        "source=table | eval identifier=mvfind(array(***,***,***),***) | fields + identifier",
        anonymize(
            "source=t | eval result=mvfind(array('apple', 'banana', 'apricot'), 'ban.*') | fields"
                + " result"));
  }

  @Test
  public void testMvcombineCommand() {
    assertEquals(
        "source=table | mvcombine delim=*** identifier", anonymize("source=t | mvcombine age"));
  }

  @Test
  public void testMvcombineCommandWithDelim() {
    assertEquals(
        "source=table | mvcombine delim=*** identifier",
        anonymize("source=t | mvcombine age delim=','"));
  }

  @Test
  public void testNoMvCommand() {
    assertEquals("source=table | nomv identifier", anonymize("source=t | nomv firstname"));
  }

  @Test
  public void testMvexpandCommand() {
    assertEquals("source=table | mvexpand identifier", anonymize("source=t | mvexpand skills"));
  }

  @Test
  public void testMvexpandCommandWithLimit() {
    assertEquals(
        "source=table | mvexpand identifier limit=***",
        anonymize("source=t | mvexpand skills limit=5"));
  }
}

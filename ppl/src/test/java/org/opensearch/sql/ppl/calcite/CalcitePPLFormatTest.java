/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLFormatTest extends CalcitePPLAbstractTest {

  public CalcitePPLFormatTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testDefaultFormat() {
    withPPLQuery("source=EMP | fields ENAME, JOB | head 2 | format")
        .expectResult(
            "search=( ( ENAME=\"SMITH\" AND JOB=\"CLERK\" ) OR ( ENAME=\"ALLEN\" AND"
                + " JOB=\"SALESMAN\" ) )\n");
  }

  @Test
  public void testLogicalAndSparkSqlPlan() {
    RelNode root = getRelNode("source=EMP | fields ENAME | format maxresults=1");
    verifyLogical(
        root,
        "LogicalProject(search=[CASE(>(CHAR_LENGTH(ARRAY_JOIN($0, ' OR ':VARCHAR)), 0), ||(||('("
            + " ':VARCHAR, ARRAY_JOIN($0, ' OR ':VARCHAR)), ' )':VARCHAR), 'NOT( )':VARCHAR)])\n"
            + "  LogicalAggregate(group=[{}], __format_rows=[ARRAY_AGG($0) FILTER $1])\n"
            + "    LogicalProject(__format_row=[CASE(>(CHAR_LENGTH(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(CASE(IS"
            + " NOT NULL($0), ||(||('ENAME=\"':VARCHAR, REPLACE(REPLACE(CAST($0):VARCHAR NOT NULL,"
            + " '\\':VARCHAR, '\\\\':VARCHAR), '\"':VARCHAR, '\\\"':VARCHAR)), '\"':VARCHAR),"
            + " null:VARCHAR))), ' AND ':VARCHAR)), 0), ||(||('( ':VARCHAR,"
            + " ARRAY_JOIN(ARRAY_COMPACT(ARRAY(CASE(IS NOT NULL($0), ||(||('ENAME=\"':VARCHAR,"
            + " REPLACE(REPLACE(CAST($0):VARCHAR NOT NULL, '\\':VARCHAR, '\\\\':VARCHAR),"
            + " '\"':VARCHAR, '\\\"':VARCHAR)), '\"':VARCHAR), null:VARCHAR))), ' AND ':VARCHAR)),"
            + " ' )':VARCHAR), null:VARCHAR)], $f1=[IS NOT"
            + " NULL(CASE(>(CHAR_LENGTH(ARRAY_JOIN(ARRAY_COMPACT(ARRAY(CASE(IS NOT NULL($0),"
            + " ||(||('ENAME=\"':VARCHAR, REPLACE(REPLACE(CAST($0):VARCHAR NOT NULL, '\\':VARCHAR,"
            + " '\\\\':VARCHAR), '\"':VARCHAR, '\\\"':VARCHAR)), '\"':VARCHAR), null:VARCHAR))), '"
            + " AND ':VARCHAR)), 0), ||(||('( ':VARCHAR, ARRAY_JOIN(ARRAY_COMPACT(ARRAY(CASE(IS NOT"
            + " NULL($0), ||(||('ENAME=\"':VARCHAR, REPLACE(REPLACE(CAST($0):VARCHAR NOT NULL,"
            + " '\\':VARCHAR, '\\\\':VARCHAR), '\"':VARCHAR, '\\\"':VARCHAR)), '\"':VARCHAR),"
            + " null:VARCHAR))), ' AND ':VARCHAR)), ' )':VARCHAR), null:VARCHAR))])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(ENAME=[$1])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT CASE WHEN CHAR_LENGTH(ARRAY_JOIN(ARRAY_AGG(`__format_row`) FILTER (WHERE `$f1`),"
            + " ' OR ')) > 0 THEN '( ' || ARRAY_JOIN(ARRAY_AGG(`__format_row`) FILTER (WHERE"
            + " `$f1`), ' OR ') || ' )' ELSE 'NOT( )' END `search`\n"
            + "FROM (SELECT CASE WHEN CHAR_LENGTH(ARRAY_JOIN(ARRAY_COMPACT(ARRAY (CASE WHEN `ENAME`"
            + " IS NOT NULL THEN 'ENAME=\"' || REPLACE(REPLACE(CAST(`ENAME` AS STRING), '\\',"
            + " '\\\\'), '\"', '\\\"') || '\"' ELSE NULL END)), ' AND ')) > 0 THEN '( ' ||"
            + " ARRAY_JOIN(ARRAY_COMPACT(ARRAY (CASE WHEN `ENAME` IS NOT NULL THEN 'ENAME=\"' ||"
            + " REPLACE(REPLACE(CAST(`ENAME` AS STRING), '\\', '\\\\'), '\"', '\\\"') || '\"'"
            + " ELSE NULL END)), ' AND ') || ' )' ELSE NULL END `__format_row`, CASE WHEN"
            + " CHAR_LENGTH(ARRAY_JOIN(ARRAY_COMPACT(ARRAY (CASE WHEN `ENAME` IS NOT NULL THEN"
            + " 'ENAME=\"' || REPLACE(REPLACE(CAST(`ENAME` AS STRING), '\\', '\\\\'), '\"',"
            + " '\\\"') || '\"' ELSE NULL END)), ' AND ')) > 0 THEN '( ' ||"
            + " ARRAY_JOIN(ARRAY_COMPACT(ARRAY (CASE WHEN `ENAME` IS NOT NULL THEN 'ENAME=\"' ||"
            + " REPLACE(REPLACE(CAST(`ENAME` AS STRING), '\\', '\\\\'), '\"', '\\\"') || '\"'"
            + " ELSE NULL END)), ' AND ') || ' )' ELSE NULL END IS NOT NULL `$f1`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1) `t1`");
  }

  @Test
  public void testCustomDelimitersAndMaxResults() {
    withPPLQuery(
            "source=EMP | fields ENAME, JOB | head 2 "
                + "| format maxresults=1 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"")
        .expectResult("search=[ [ ENAME=\"SMITH\" && JOB=\"CLERK\" ] ]\n");
  }

  @Test
  public void testMultivalueFormat() {
    withPPLQuery(
            "source=EMP | head 1 | eval tags=array(\"critical\", \"network\") "
                + "| fields tags | format mvsep=\"mvseparator\" "
                + "\"{\" \"[\" \"AND\" \"]\" \"AND\" \"}\"")
        .expectResult("search={ [ ( tags=\"critical\" mvseparator tags=\"network\" ) ] }\n");
  }

  @Test
  public void testEmptyMultivalueUsesFallback() {
    withPPLQuery(
            "source=EMP | head 1 | eval tags=array() | fields tags "
                + "| format emptystr=\"empty\"")
        .expectResult("search=empty\n");
  }

  @Test
  public void testEmptyResultFallback() {
    withPPLQuery(
            "source=EMP | where EMPNO < 0 | fields ENAME | format emptystr=\"no matching data\"")
        .expectResult("search=no matching data\n");
  }

  @Test
  public void testNullOnlyRowUsesFallback() {
    withPPLQuery("source=EMP | where EMPNO=7369 | fields COMM | format emptystr=\"empty\"")
        .expectResult("search=empty\n");
  }

  @Test
  public void testQuotesInValuesAreEscaped() {
    withPPLQuery("source=EMP | head 1 | eval message='say \\\"hi\\\"' | fields message | format")
        .expectResult("search=( ( message=\"say \\\"hi\\\"\" ) )\n");
  }

  @Test
  public void testBackslashesInValuesAreEscaped() {
    withPPLQuery("source=EMP | head 1 | eval path='a\\\\b' | fields path | format")
        .expectResult("search=( ( path=\"a\\\\b\" ) )\n");
  }

  @Test
  public void testInternalFieldsAreIgnored() {
    withPPLQuery(
            "source=EMP | head 1 | eval _private='hidden', visible='shown' "
                + "| fields _private, visible | format")
        .expectResult("search=( ( visible=\"shown\" ) )\n");
  }

  @Test
  public void testSearchAndQueryFieldsDropFieldName() {
    withPPLQuery(
            "source=EMP | head 1 | eval search='status=200', query='method=GET', a='x' "
                + "| fields search, query, a | format")
        .expectResult("search=( ( a=\"x\" AND \"method=GET\" AND \"status=200\" ) )\n");
  }

  @Test
  public void testSpecialCharactersInFieldNameAreQuoted() {
    withPPLQuery("source=EMP | head 1 | eval a.b='x' | fields a.b | format")
        .expectResult("search=( ( \"a.b\"=\"x\" ) )\n");
  }

  @Test
  public void testAllSixDelimitersAreRequired() {
    assertThrows(
        RuntimeException.class,
        () -> getRelNode("source=EMP | fields ENAME | format \"[\" \"[\" \"AND\""));
  }
}

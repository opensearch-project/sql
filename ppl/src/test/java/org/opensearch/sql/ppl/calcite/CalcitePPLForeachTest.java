/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLForeachTest extends CalcitePPLAbstractTest {

  public CalcitePPLForeachTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testForeachExpandsExplicitFields() {
    String ppl =
        "source=EMP | foreach SAL COMM [ eval <<FIELD>>_double = <<FIELD>> * 2 ] | fields"
            + " EMPNO, SAL_double, COMM_double";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], SAL_double=[*($5, 2)], COMM_double=[*($6, 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `SAL` * 2 `SAL_double`, `COMM` * 2 `COMM_double`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testForeachWildcardAndMatchstr() {
    String ppl =
        "source=EMP | foreach *NO [ eval copy_<<MATCHSTR>> = <<FIELD>> ] | fields copy_EMP,"
            + " copy_DEPT";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(copy_EMP=[$0], copy_DEPT=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachCustomMultifieldPlaceholders() {
    String ppl =
        "source=EMP | foreach fieldstr=F matchstr=M *NO [ eval copy_<<M>> = F ] | fields"
            + " copy_EMP, copy_DEPT";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(copy_EMP=[$0], copy_DEPT=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachMultivaluePlansReduce() {
    String ppl =
        "source=EMP | eval nums = array(1, 2, 3), total = 0 | foreach mode=multivalue"
            + " itemstr=NUMBER nums [ eval total = total + NUMBER ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(array(1, 2, 3)),"
            + " foreach_state(0), (__foreach_state, __foreach_pair) ->"
            + " foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachIterPlaceholderPlansReduce() {
    String ppl =
        "source=EMP | eval nums = array(10, 20, 30), total = 0 | foreach mode=multivalue"
            + " iterstr=IDX nums [ eval total = total + IDX ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(array(10, 20, 30)),"
            + " foreach_state(0), (__foreach_state, __foreach_pair) ->"
            + " foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 1)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachJsonArrayPlansReduce() {
    String ppl =
        "source=EMP | eval total = 0 | foreach mode=json_array '[1,2,3]' [ eval total = total +"
            + " <<ITEM>> ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(foreach_json_array('[1,2,3]':VARCHAR,"
            + " 'DOUBLE':VARCHAR)), foreach_state(0.0E0:DOUBLE), (__foreach_state, __foreach_pair)"
            + " -> foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachJsonArrayFunctionWithoutModePlansReduce() {
    String ppl =
        "source=EMP | eval total = 0 | foreach json_array(1, 2, 3) [ eval total = total +"
            + " <<ITEM>> ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(foreach_json_array(JSON_ARRAY(FLAG(NULL_ON_NULL),"
            + " 1, 2, 3), 'DOUBLE':VARCHAR)), foreach_state(0.0E0:DOUBLE), (__foreach_state,"
            + " __foreach_pair) -> foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachJsonArrayMixedStringAndNumberFails() {
    String ppl =
        "source=EMP | eval total = 0 | foreach json_array(1, 2, 'hello') [ eval total = total +"
            + " <<ITEM>> ] | fields total";

    assertThrows(SemanticCheckException.class, () -> getRelNode(ppl));
  }

  @Test
  public void testForeachAutoCollectionsPlansReduce() {
    String ppl =
        "source=EMP | eval nums = array(1, 2, 3), total = 0 | foreach mode=auto_collections nums ["
            + " eval total = total + <<ITEM>> ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(array(1, 2, 3)),"
            + " foreach_state(0), (__foreach_state, __foreach_pair) ->"
            + " foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachAutoCollectionsWithoutTargetPlansReduce() {
    String ppl =
        "source=EMP | eval nums = array(1, 2, 3), total = 0 | foreach mode=auto_collections ["
            + " eval total = total + <<ITEM>> ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(array(1, 2, 3)),"
            + " foreach_state(0), (__foreach_state, __foreach_pair) ->"
            + " foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachStringItemWithNumericIterPlansReduce() {
    String ppl =
        "source=EMP | eval word = split('ABCDE', ''), nums = array('1', '2', '3', '4', '5'),"
            + " word_and_num = array() | foreach word mode=multivalue [ eval word_and_num ="
            + " mvappend(word_and_num, concat(<<ITEM>>, mvindex(nums, <<ITER>>))) ] | fields"
            + " word_and_num";
    RelNode root = getRelNode(ppl);

    // The captured field `nums` rides in pair slot 2; ITEM is slot 0 and ITER slot 1.
    String expectedLogical =
        "LogicalProject(word_and_num=[foreach_pair_item(reduce(foreach_pair_collection(CASE(=('':VARCHAR,"
            + " ''), REGEXP_EXTRACT_ALL('ABCDE':VARCHAR, '.'), SPLIT('ABCDE':VARCHAR, '':VARCHAR)),"
            + " array('1', '2', '3', '4', '5')), foreach_state(array()), (__foreach_state,"
            + " __foreach_pair) -> foreach_state(mvappend(foreach_pair_item(__foreach_state, 0),"
            + " CONCAT(foreach_pair_item(__foreach_pair, 0), ITEM(foreach_pair_item(__foreach_pair,"
            + " 2), CASE(<(foreach_pair_item(__foreach_pair, 1), 0),"
            + " +(+(ARRAY_LENGTH(foreach_pair_item(__foreach_pair, 2)),"
            + " foreach_pair_item(__foreach_pair, 1)), 1), +(foreach_pair_item(__foreach_pair, 1),"
            + " 1))))))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachJsonArrayFieldNumericUsage() {
    // Field content is opaque at plan time; arithmetic on <<ITEM>> selects DOUBLE elements.
    String ppl =
        "source=EMP | eval total = 0 | foreach mode=json_array ENAME [ eval total = total +"
            + " <<ITEM>> ] | fields total";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(total=[foreach_pair_item(reduce(foreach_pair_collection(foreach_json_array($1,"
            + " 'DOUBLE':VARCHAR)), foreach_state(0.0E0:DOUBLE), (__foreach_state, __foreach_pair)"
            + " -> foreach_state(+(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachJsonArrayFieldStringUsage() {
    // No arithmetic on <<ITEM>> keeps field-backed JSON array elements as VARCHAR.
    String ppl =
        "source=EMP | eval r = '' | foreach mode=json_array ENAME [ eval r = concat(r, <<ITEM>>)"
            + " ] | fields r";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(r=[foreach_pair_item(reduce(foreach_pair_collection(foreach_json_array($1,"
            + " 'VARCHAR':VARCHAR)), foreach_state('':VARCHAR), (__foreach_state, __foreach_pair)"
            + " -> foreach_state(CONCAT(foreach_pair_item(__foreach_state, 0),"
            + " foreach_pair_item(__foreach_pair, 0)))), 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testForeachSubstitutesTemplateInsideStringLiteral() {
    RelNode root =
        getRelNode("source=EMP | foreach ENAME [ eval copy = '<<FIELD>>' ] | fields copy");

    verifyLogical(
        root, "LogicalProject(copy=['ENAME'])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testDefaultItemNameDoesNotShadowRowField() {
    RelNode root =
        getRelNode(
            "source=EMP | eval ITEM=100, nums=array(1,2), total=0 | foreach mode=multivalue"
                + " nums [ eval total=total+ITEM ] | fields total");

    String logical = org.apache.calcite.plan.RelOptUtil.toString(root);
    assertTrue(logical.contains("foreach_pair_collection(array(1, 2), 100)"));
    assertTrue(logical.contains("foreach_pair_item(__foreach_pair, 2)"));
  }

  @Test
  public void testCustomItemNameShadowsRowField() {
    RelNode root =
        getRelNode(
            "source=EMP | eval ITEM=100, nums=array(1,2), total=0 | foreach mode=multivalue"
                + " itemstr=ITEM nums [ eval total=total+ITEM ] | fields total");

    String logical = org.apache.calcite.plan.RelOptUtil.toString(root);
    assertTrue(logical.contains("foreach_pair_collection(array(1, 2))"));
    assertTrue(logical.contains("foreach_pair_item(__foreach_pair, 0)"));
  }

  @Test
  public void testJsonFieldNumericFunctionInfersDoubleItem() {
    RelNode root =
        getRelNode(
            "source=EMP | eval result=0 | foreach mode=json_array ENAME [ eval"
                + " result=abs(<<ITEM>>) ] | fields result");

    String logical = org.apache.calcite.plan.RelOptUtil.toString(root);
    assertTrue(logical, logical.contains("foreach_json_array($1, 'DOUBLE':VARCHAR)"));
  }

  @Test
  public void testJsonFieldNumericComparisonInfersDoubleItem() {
    RelNode root =
        getRelNode(
            "source=EMP | eval count=0 | foreach mode=json_array ENAME [ eval"
                + " count=count+if(<<ITEM>> > 9, 1, 0) ] | fields count");

    assertTrue(
        org.apache.calcite.plan.RelOptUtil.toString(root)
            .contains("foreach_json_array($1, 'DOUBLE':VARCHAR)"));
  }

  @Test
  public void testCollectionModeMismatchIsNoOp() {
    RelNode multivalue =
        getRelNode(
            "source=EMP | eval total=0 | foreach mode=multivalue ENAME [ eval"
                + " total=total+<<ITEM>> ] | fields total");
    RelNode jsonArray =
        getRelNode(
            "source=EMP | eval nums=array(1,2), total=0 | foreach mode=json_array nums [ eval"
                + " total=total+<<ITEM>> ] | fields total");

    String expected = "LogicalProject(total=[0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(multivalue, expected);
    verifyLogical(jsonArray, expected);
  }

  @Test
  public void testExactPatternTakesPrecedenceForMatchstr() {
    RelNode root =
        getRelNode(
            "source=EMP | foreach *NO EMPNO [ eval copy_<<MATCHSTR>> = '<<MATCHSTR>>' ] |"
                + " fields copy_");

    verifyLogical(
        root, "LogicalProject(copy_=[''])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testCollectionAssignmentsSharePerIterationState() {
    RelNode root =
        getRelNode(
            "source=EMP | eval nums=array(1,2), sum=0, seen=0 | foreach mode=multivalue nums"
                + " [ eval sum=sum+<<ITEM>>, seen=seen+sum ] | fields sum, seen");

    String logical = org.apache.calcite.plan.RelOptUtil.toString(root);
    assertTrue(logical.contains("foreach_state(0, 0)"));
    assertTrue(
        logical.contains(
            "foreach_state(+(foreach_pair_item(__foreach_state, 0),"
                + " foreach_pair_item(__foreach_pair, 0)),"
                + " +(foreach_pair_item(__foreach_state, 1),"
                + " +(foreach_pair_item(__foreach_state, 0),"
                + " foreach_pair_item(__foreach_pair, 0))))"));
  }
}

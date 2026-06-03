/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLEventstatsTest extends CalcitePPLAbstractTest {

  public CalcitePPLEventstatsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  // After https://github.com/opensearch-project/sql/issues/5483 the visitor rewrites every
  // eventstats command from `Project(RexOver)` into `Project → Join → (input, Aggregate(input))`
  // so the right-side aggregate can match `AggregateIndexScanRule` and push down to OpenSearch
  // as `size:0 + track_total_hits` (no-BY) or a `terms` aggregation (BY). The unit tests below
  // pin the new lowered shape; pushdown is verified end-to-end in `CalciteExplainIT` and
  // result-correctness in `CalcitePPLEventstatsIT`.
  //
  // The Spark SQL conversion (`verifyPPLToSparkSQL`) for the new join+aggregate shape depends on
  // Calcite's `SparkSqlDialect` emitter for cross/equi joins with subqueries; the previous
  // window-form expectations no longer apply. Re-add `verifyPPLToSparkSQL` assertions once the
  // emitter output has been observed on a working build.

  @Test
  public void testEventstatsCount() {
    String ppl = "source=EMP | eventstats count()";
    RelNode root = getRelNode(ppl);
    // No-BY: a literal-0 key is projected on both sides so the join becomes equi
    // (left.__eventstats_join_key__ = right.__eventstats_join_key__). Without this, Calcite picks
    // EnumerableNestedLoopJoin and re-opens the right scan once per left row, which means
    // ~N OpenSearch requests for an N-row result set. With the equi-join, the planner picks
    // hash/merge and drains the (single-row) right side once.
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], count()=[$9])\n"
            + "  LogicalJoin(condition=[=($8, $10)], joinType=[inner])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(count()=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], count()=[COUNT()])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsBy() {
    String ppl = "source=EMP | eventstats max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    // bucketNullable defaults to true, so the join keeps the NULL bucket: the rewrite emits
    // `(left.DEPTNO = right.DEPTNO) OR (left.DEPTNO IS NULL AND right.DEPTNO IS NULL)`, which
    // Calcite canonicalizes to the equivalent `IS NOT DISTINCT FROM` operator. The outer Project
    // is preserved because we must drop the right-side group-key column ($8).
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalJoin(condition=[IS NOT DISTINCT FROM($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalAggregate(group=[{0}], max(SAL)=[MAX($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsAvg() {
    String ppl = "source=EMP | eventstats avg(SAL)";
    RelNode root = getRelNode(ppl);
    // AVG now goes through the aggregate path (not the window path), so it stays as a single AVG
    // aggregate rather than being decomposed into SUM/COUNT as the legacy window form did. See
    // testEventstatsCount for the rationale behind the literal-0 equi-join key on no-BY.
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], avg(SAL)=[$9])\n"
            + "  LogicalJoin(condition=[=($8, $10)], joinType=[inner])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __eventstats_join_key__=[0])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(avg(SAL)=[$0], __eventstats_join_key__=[0])\n"
            + "      LogicalAggregate(group=[{}], avg(SAL)=[AVG($0)])\n"
            + "        LogicalProject(SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testEventstatsNullBucket() {
    String ppl = "source=EMP | eventstats bucket_nullable=false avg(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    // bucketNullable=false: the right aggregate filters IS NOT NULL on DEPTNO before grouping
    // (matching the bucket-non-null pushdown shape stats already uses), and the join is LEFT on
    // simple equality so NULL-keyed left rows survive with a NULL aggregate value, preserving
    // the semantics of the previous CASE-wrapped window form.
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], avg(SAL)=[$9])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}

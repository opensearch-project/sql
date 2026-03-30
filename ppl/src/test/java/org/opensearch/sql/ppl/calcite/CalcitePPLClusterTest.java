/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLClusterTest extends CalcitePPLAbstractTest {

  public CalcitePPLClusterTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testBasicCluster() {
    String ppl = "source=EMP | cluster ENAME";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[$8])\n"
            + "  LogicalFilter(condition=[=($9, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[$8],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termlist':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, ROW_NUMBER() OVER (PARTITION BY `cluster_label`)"
            + " `_cluster_convergence_row_num`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric') OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`) `t1`) `t2`\n"
            + "WHERE `_cluster_convergence_row_num` = 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testClusterWithThreshold() {
    String ppl = "source=EMP | cluster ENAME t=0.8";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[$8])\n"
            + "  LogicalFilter(condition=[=($9, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[$8],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termlist':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithTermsetMatch() {
    String ppl = "source=EMP | cluster ENAME match=termset";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[$8])\n"
            + "  LogicalFilter(condition=[=($9, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[$8],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termset':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithNgramsetMatch() {
    String ppl = "source=EMP | cluster ENAME match=ngramset";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[$8])\n"
            + "  LogicalFilter(condition=[=($9, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[$8],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'ngramset':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithCustomFields() {
    String ppl = "source=EMP | cluster ENAME labelfield=my_cluster countfield=my_count";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], my_cluster=[$8])\n"
            + "  LogicalFilter(condition=[=($9, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], my_cluster=[$8],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], my_cluster=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termlist':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithAllParameters() {
    String ppl =
        "source=EMP | cluster ENAME t=0.7 match=termset labelfield=cluster_id"
            + " countfield=cluster_size showcount=true labelonly=false delims=' '";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_id=[$8], cluster_size=[$9])\n"
            + "  LogicalFilter(condition=[=($10, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_id=[$8], cluster_size=[$9],"
            + " _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_id=[$8], cluster_size=[COUNT() OVER"
            + " (PARTITION BY $8)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_id=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.7E0:DOUBLE, 'termset':VARCHAR, ' ') OVER ()])\n"
            + "            LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterOnDifferentField() {
    String ppl = "source=EMP | cluster JOB";
    RelNode root = getRelNode(ppl);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, ROW_NUMBER() OVER (PARTITION BY `cluster_label`)"
            + " `_cluster_convergence_row_num`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`JOB`, 8E-1, 'termlist', 'non-alphanumeric') OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `JOB` IS NOT NULL) `t0`) `t1`) `t2`\n"
            + "WHERE `_cluster_convergence_row_num` = 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testClusterLabelOnly() {
    String ppl = "source=EMP | cluster ENAME labelonly=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termlist':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric') OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testClusterLabelOnlyWithShowCount() {
    String ppl = "source=EMP | cluster ENAME labelonly=true showcount=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], cluster_label=[$8], cluster_count=[COUNT() OVER"
            + " (PARTITION BY $8)])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER"
            + " ()):INTEGER NOT NULL)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _cluster_labels_array=[cluster_label($1,"
            + " 0.8E0:DOUBLE, 'termlist':VARCHAR, 'non-alphanumeric':VARCHAR) OVER ()])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, COUNT(*) OVER (PARTITION BY `cluster_label` RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `cluster_count`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric') OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}

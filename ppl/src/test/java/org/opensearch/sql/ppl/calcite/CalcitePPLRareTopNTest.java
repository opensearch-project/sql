/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLRareTopNTest extends CalcitePPLAbstractTest {

  public CalcitePPLRareTopNTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testRare() {
    String ppl = "source=EMP | rare JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        """
                    LogicalProject(JOB=[$0], count=[$1])
                      LogicalFilter(condition=[<=($2, 10)])
                        LogicalProject(JOB=[$0], count=[$1], _row_number_=[ROW_NUMBER() OVER (ORDER BY\
                     $1)])
                          LogicalAggregate(group=[{0}], count=[COUNT()])
                            LogicalProject(JOB=[$2])
                              LogicalTableScan(table=[[scott, EMP]])
                    """;
    verifyLogical(root, expectedLogical);

    String expectedResult =
        """
                    JOB=PRESIDENT; count=1
                    JOB=ANALYST; count=2
                    JOB=MANAGER; count=3
                    JOB=SALESMAN; count=4
                    JOB=CLERK; count=4
                    """;
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        """
                    SELECT `JOB`, `count`
                    FROM (SELECT `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (ORDER BY COUNT(*) NULLS\
                     LAST) `_row_number_`
                    FROM `scott`.`EMP`
                    GROUP BY `JOB`) `t1`
                    WHERE `_row_number_` <= 10""";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareBy() {
    String ppl = "source=EMP | rare JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        """
                    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2])
                      LogicalFilter(condition=[<=($3, 10)])
                        LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], _row_number_=[ROW_NUMBER()\
                     OVER (PARTITION BY $0 ORDER BY $2)])
                          LogicalAggregate(group=[{0, 1}], count=[COUNT()])
                            LogicalProject(DEPTNO=[$7], JOB=[$2])
                              LogicalTableScan(table=[[scott, EMP]])
                    """;
    verifyLogical(root, expectedLogical);

    String expectedResult =
        """
                    DEPTNO=20; JOB=MANAGER; count=1
                    DEPTNO=20; JOB=CLERK; count=2
                    DEPTNO=20; JOB=ANALYST; count=2
                    DEPTNO=10; JOB=MANAGER; count=1
                    DEPTNO=10; JOB=CLERK; count=1
                    DEPTNO=10; JOB=PRESIDENT; count=1
                    DEPTNO=30; JOB=MANAGER; count=1
                    DEPTNO=30; JOB=CLERK; count=1
                    DEPTNO=30; JOB=SALESMAN; count=4
                    """;
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        """
                    SELECT `DEPTNO`, `JOB`, `count`
                    FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY\
                     `DEPTNO` ORDER BY COUNT(*) NULLS LAST) `_row_number_`
                    FROM `scott`.`EMP`
                    GROUP BY `DEPTNO`, `JOB`) `t1`
                    WHERE `_row_number_` <= 10""";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareDisableShowCount() {
    String ppl = "source=EMP | rare showcount=false JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        """
                    LogicalProject(DEPTNO=[$0], JOB=[$1])
                      LogicalFilter(condition=[<=($3, 10)])
                        LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], _row_number_=[ROW_NUMBER()\
                     OVER (PARTITION BY $0 ORDER BY $2)])
                          LogicalAggregate(group=[{0, 1}], count=[COUNT()])
                            LogicalProject(DEPTNO=[$7], JOB=[$2])
                              LogicalTableScan(table=[[scott, EMP]])
                    """;
    verifyLogical(root, expectedLogical);

    String expectedResult =
        """
                    DEPTNO=20; JOB=MANAGER
                    DEPTNO=20; JOB=CLERK
                    DEPTNO=20; JOB=ANALYST
                    DEPTNO=10; JOB=MANAGER
                    DEPTNO=10; JOB=CLERK
                    DEPTNO=10; JOB=PRESIDENT
                    DEPTNO=30; JOB=MANAGER
                    DEPTNO=30; JOB=CLERK
                    DEPTNO=30; JOB=SALESMAN
                    """;
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        """
                    SELECT `DEPTNO`, `JOB`
                    FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY\
                     `DEPTNO` ORDER BY COUNT(*) NULLS LAST) `_row_number_`
                    FROM `scott`.`EMP`
                    GROUP BY `DEPTNO`, `JOB`) `t1`
                    WHERE `_row_number_` <= 10""";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareCountField() {
    String ppl = "source=EMP | rare countfield='my_cnt' JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        """
                    LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2])
                      LogicalFilter(condition=[<=($3, 10)])
                        LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2], _row_number_=[ROW_NUMBER()\
                     OVER (PARTITION BY $0 ORDER BY $2)])
                          LogicalAggregate(group=[{0, 1}], my_cnt=[COUNT()])
                            LogicalProject(DEPTNO=[$7], JOB=[$2])
                              LogicalTableScan(table=[[scott, EMP]])
                    """;
    verifyLogical(root, expectedLogical);

    String expectedResult =
        """
                    DEPTNO=20; JOB=MANAGER; my_cnt=1
                    DEPTNO=20; JOB=CLERK; my_cnt=2
                    DEPTNO=20; JOB=ANALYST; my_cnt=2
                    DEPTNO=10; JOB=MANAGER; my_cnt=1
                    DEPTNO=10; JOB=CLERK; my_cnt=1
                    DEPTNO=10; JOB=PRESIDENT; my_cnt=1
                    DEPTNO=30; JOB=MANAGER; my_cnt=1
                    DEPTNO=30; JOB=CLERK; my_cnt=1
                    DEPTNO=30; JOB=SALESMAN; my_cnt=4
                    """;
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        """
                    SELECT `DEPTNO`, `JOB`, `my_cnt`
                    FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `my_cnt`, ROW_NUMBER() OVER (PARTITION BY\
                     `DEPTNO` ORDER BY COUNT(*) NULLS LAST) `_row_number_`
                    FROM `scott`.`EMP`
                    GROUP BY `DEPTNO`, `JOB`) `t1`
                    WHERE `_row_number_` <= 10""";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void failWithDuplicatedName() {
    try {
      RelNode root = getRelNode("source=EMP | eval count=1 | rare JOB by count");
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(
          e.getMessage(),
          is(
              "The top/rare output field `count` already exists. Suggestion: change the count field"
                  + " by adding countfield='xyz'"));
    }
  }

  @Test
  public void failWithDuplicateOverriddenName() {
    try {
      RelNode root = getRelNode("source=EMP | rare countfield='DEPTNO' JOB by DEPTNO");
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(
          e.getMessage(),
          is(
              "The top/rare output field `DEPTNO` already exists. Suggestion: change the count"
                  + " field by adding countfield='xyz'"));
    }
  }
}

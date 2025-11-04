/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLEventstatsTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLEventstatsTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCountWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats count(EMPNO, DEPTNO)",
        "Aggregation function COUNT expects field type and additional arguments {[]|[ANY]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testAvgWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats avg(EMPNO, DEPTNO)",
        "Aggregation function AVG expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testSumWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats sum(EMPNO, DEPTNO)",
        "Aggregation function SUM expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testMinWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats min(EMPNO, DEPTNO)",
        "Aggregation function MIN expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testMaxWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats max(EMPNO, DEPTNO)",
        "Aggregation function MAX expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats var_samp(EMPNO, DEPTNO)",
        "Aggregation function VARSAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testVarPopWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats var_pop(EMPNO, DEPTNO)",
        "Aggregation function VARPOP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testStddevSampWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats stddev_samp(EMPNO, DEPTNO)",
        "Aggregation function STDDEV_SAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testStddevPopWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats stddev_pop(EMPNO, DEPTNO)",
        "Aggregation function STDDEV_POP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testEarliestWithTooManyParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats earliest(ENAME, HIREDATE, JOB)",
        "Aggregation function EARLIEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,DATE,STRING]");
  }

  @Test
  public void testLatestWithTooManyParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | eventstats latest(ENAME, HIREDATE, JOB)",
        "Aggregation function LATEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,DATE,STRING]");
  }

  @Test
  public void testAvgWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | eventstats avg(HIREDATE) as avg_name",
        "Aggregation function AVG expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testVarsampWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | eventstats var_samp(HIREDATE) as varsamp_name",
        "Aggregation function VARSAMP expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testVarpopWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | eventstats var_pop(HIREDATE) as varpop_name",
        "Aggregation function VARPOP expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testStddevSampWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | eventstats stddev_samp(HIREDATE) as stddev_name",
        "Aggregation function STDDEV_SAMP expects field type {[INTEGER]|[DOUBLE]}, but got"
            + " [DATE]");
  }

  @Test
  public void testStddevPopWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | eventstats stddev_pop(HIREDATE) as stddev_name",
        "Aggregation function STDDEV_POP expects field type {[INTEGER]|[DOUBLE]}, but got"
            + " [DATE]");
  }
}

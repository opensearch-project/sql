/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAggregateFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLAggregateFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAvgWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats avg(HIREDATE) as avg_name",
        "Aggregation function AVG expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testVarsampWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats var_samp(HIREDATE) as varsamp_name",
        "Aggregation function VARSAMP expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testVarpopWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats var_pop(HIREDATE) as varpop_name",
        "Aggregation function VARPOP expects field type {[INTEGER]|[DOUBLE]}, but got [DATE]");
  }

  @Test
  public void testStddevSampWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats stddev_samp(HIREDATE) as stddev_name",
        "Aggregation function STDDEV_SAMP expects field type {[INTEGER]|[DOUBLE]}, but got"
            + " [DATE]");
  }

  @Test
  public void testStddevPopWithWrongArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats stddev_pop(HIREDATE) as stddev_name",
        "Aggregation function STDDEV_POP expects field type {[INTEGER]|[DOUBLE]}, but got"
            + " [DATE]");
  }

  @Test
  public void testPercentileApproxWithWrongArgType() {
    // First argument should be numeric
    verifyQueryThrowsException(
        "source=EMP | stats percentile_approx(HIREDATE, 50) as percentile",
        "Aggregation function PERCENTILE_APPROX expects field type and additional arguments"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]|[INTEGER,INTEGER,INTEGER]|[INTEGER,INTEGER,DOUBLE]|[INTEGER,DOUBLE,INTEGER]|[INTEGER,DOUBLE,DOUBLE]|[DOUBLE,INTEGER,INTEGER]|[DOUBLE,INTEGER,DOUBLE]|[DOUBLE,DOUBLE,INTEGER]|[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [DATE,INTEGER]");
  }

  @Test
  public void testListFunctionWithArrayArgType() {
    // Test LIST function with array expression (which is not a supported scalar type)
    verifyQueryThrowsException(
        "source=EMP | stats list(array(ENAME, JOB)) as name_list",
        "Aggregation function LIST expects field type"
            + " {[BYTE]|[SHORT]|[INTEGER]|[LONG]|[FLOAT]|[DOUBLE]|[STRING]|[BOOLEAN]|[DATE]|[TIME]|[TIMESTAMP]|[IP]|[BINARY]},"
            + " but got [ARRAY]");
  }

  @Test
  public void testCountWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats count(EMPNO, DEPTNO)",
        "Aggregation function COUNT expects field type and additional arguments {[]|[ANY]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testAvgWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats avg(EMPNO, DEPTNO)",
        "Aggregation function AVG expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testSumWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats sum(EMPNO, DEPTNO)",
        "Aggregation function SUM expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testMinWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats min(EMPNO, DEPTNO)",
        "Aggregation function MIN expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testMaxWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats max(EMPNO, DEPTNO)",
        "Aggregation function MAX expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [SHORT,BYTE]");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats var_samp(EMPNO, DEPTNO)",
        "Aggregation function VARSAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testVarPopWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats var_pop(EMPNO, DEPTNO)",
        "Aggregation function VARPOP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testStddevSampWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats stddev_samp(EMPNO, DEPTNO)",
        "Aggregation function STDDEV_SAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testStddevPopWithExtraParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats stddev_pop(EMPNO, DEPTNO)",
        "Aggregation function STDDEV_POP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [SHORT,BYTE]");
  }

  @Test
  public void testPercentileWithMissingParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats percentile(EMPNO)",
        "Aggregation function PERCENTILE_APPROX expects field type"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]|[INTEGER,INTEGER,INTEGER]|[INTEGER,INTEGER,DOUBLE]|[INTEGER,DOUBLE,INTEGER]|[INTEGER,DOUBLE,DOUBLE]|[DOUBLE,INTEGER,INTEGER]|[DOUBLE,INTEGER,DOUBLE]|[DOUBLE,DOUBLE,INTEGER]|[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [SHORT]");
  }

  @Test
  public void testPercentileWithInvalidParameterTypesThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats percentile(EMPNO, 50, HIREDATE)",
        "Aggregation function PERCENTILE_APPROX expects field type and additional arguments"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]|[INTEGER,INTEGER,INTEGER]|[INTEGER,INTEGER,DOUBLE]|[INTEGER,DOUBLE,INTEGER]|[INTEGER,DOUBLE,DOUBLE]|[DOUBLE,INTEGER,INTEGER]|[DOUBLE,INTEGER,DOUBLE]|[DOUBLE,DOUBLE,INTEGER]|[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [SHORT,INTEGER,DATE]");
  }

  @Test
  public void testEarliestWithTooManyParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats earliest(ENAME, HIREDATE, JOB)",
        "Aggregation function EARLIEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,DATE,STRING]");
  }

  @Test
  public void testLatestWithTooManyParametersThrowsException() {
    verifyQueryThrowsException(
        "source=EMP | stats latest(ENAME, HIREDATE, JOB)",
        "Aggregation function LATEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,DATE,STRING]");
  }
}

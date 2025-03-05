/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMathFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLMathFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAbsWithOverriding() {
    String ppl = "source=EMP | eval SAL = abs(-30) | head 10 | fields SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL0=[$7])\n"
            + "  LogicalSort(fetch=[10])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " COMM=[$6], DEPTNO=[$7], SAL0=[ABS(-30)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT ABS(-30) `SAL0`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAcosWithOverriding() {
    String ppl = "source=EMP | eval ACOS = acos(0) | head 2 | fields ACOS";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "LogicalProject(ACOS=[$8])\n" +
            "  LogicalSort(fetch=[2])\n" +
            "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4]," +
            " SAL=[$5], COMM=[$6], DEPTNO=[$7], ACOS=[ACOS(0)])\n" +
            "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ACOS=1.5707963267948966\n" + "ACOS=1.5707963267948966\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT ACOS(0) `ACOS`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAsinWithOverriding() {
    String ppl = "source=EMP | eval ASIN = asin(0) | head 2 | fields ASIN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ASIN=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ASIN=[ASIN(0)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ASIN=0.0\n" + "ASIN=0.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT ASIN(0) `ASIN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtanWithOverriding() {
    String ppl = "source=EMP | eval ATAN = atan(2) | head 2 | fields ATAN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ATAN=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ATAN=[ATAN2(2, 1:BIGINT)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ATAN=1.1071487177940904\n" + "ATAN=1.1071487177940904\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT ATAN2(2, 1) `ATAN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtan2WithOverriding() {
    String ppl = "source=EMP | eval ATAN2 = atan(2, 3) | head 2 | fields ATAN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ATAN2=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ATAN2=[ATAN2(2, 3)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ATAN2=0.5880026035475675\n" + "ATAN2=0.5880026035475675\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT ATAN2(2, 3) `ATAN2`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCbrtWithOverriding() {
    String ppl = "source=EMP | eval CBRT = cbrt(27) | head 2 | fields CBRT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(CBRT=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], CBRT=[CBRT(27)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "CBRT=3.0\n" + "CBRT=3.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT CBRT(27) `CBRT`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCeilingWithOverriding() {
    String ppl = "source=EMP | eval CEILING = ceiling(4.5) | head 2 | fields CEILING";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
            "LogicalProject(CEILING=[$8])\n"
                    + "  LogicalSort(fetch=[2])\n"
                    + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
                    + " SAL=[$5], COMM=[$6], DEPTNO=[$7], CEILING=[CEIL(4.5E0:DOUBLE)])\n"
                    + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "CEILING=5.0\n" + "CEILING=5.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
            "" + "SELECT CEIL(4.5E0) `CEILING`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvWithOverriding() {
    String ppl = "source=EMP | eval CONV = conv(10, 10, 2) | head 2 | fields CONV";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(CONV=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], CONV=[CONVERT(10, 10, 2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "CONV=1010\n" + "CONV=1010\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "" + "SELECT CONVERT(10, 10, 2) `CONV`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCosWithOverriding() {
    String ppl = "source=EMP | eval COS = cos(0) | head 2 | fields COS";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(COS=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], COS=[COS(0)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "COS=1.0\n" + "COS=1.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT COS(0) `COS`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCotWithOverriding() {
    String ppl = "source=EMP | eval COT = cot(1) | head 2 | fields COT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(COT=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], COT=[COT(1)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "COT=0.6420926159343306\n" + "COT=0.6420926159343306\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testCrc32WithOverrding() {
    String ppl = "source=EMP | eval CRC32TEST = crc32('test') | head 2 | fields CRC32TEST";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
            "LogicalProject(CRC32TEST=[$8])\n"
                    + "  LogicalSort(fetch=[2])\n"
                    + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
                    + " SAL=[$5], COMM=[$6], DEPTNO=[$7], CRC32TEST=[CRC32('test')])\n"
                    + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "CRC32TEST=3632233996\n" + "CRC32TEST=3632233996\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
            "SELECT `CRC32`('test') `CRC32TEST`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDegreesWithOverriding() {
    String ppl = "source=EMP | eval DEGREES = degrees(1.57) | head 2 | fields DEGREES";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEGREES=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEGREES=[DEGREES(1.57E0:DOUBLE)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "DEGREES=89.95437383553924\n" + "DEGREES=89.95437383553924\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT DEGREES(1.57E0) `DEGREES`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEulerWithOverriding() {
    String ppl = "source=EMP | eval EULER = e() | head 2 | fields EULER";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EULER=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], EULER=[E()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "EULER=2.718281828459045\n" + "EULER=2.718281828459045\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT `E`() `EULER`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExpWithOverriding() {
    String ppl = "source=EMP | eval EXP = exp(2) | head 1 | fields EXP";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EXP=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], EXP=[EXP(2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "EXP=7.38905609893065\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT EXP(2) `EXP`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFloorWithOverriding() {
    String ppl = "source=EMP | eval FLOOR1 = floor(50.00005), FLOOR2 = floor(-50.0005) | head 1 | fields FLOOR1, FLOOR2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(FLOOR1=[$8], FLOOR2=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], FLOOR1=[FLOOR(50.00005E0:DOUBLE)], FLOOR2=[FLOOR(-50.0005E0:DOUBLE)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "FLOOR1=50.0; FLOOR2=-51.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT FLOOR(5.000005E1) `FLOOR1`, FLOOR(-5.00005E1) `FLOOR2`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLnWithOverriding() {
    String ppl = "source=EMP | eval LN = ln(2) | head 1 | fields LN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(LN=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], LN=[LN(2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "LN=0.6931471805599453\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT LN(2) `LN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLogWithOverriding() {
    String ppl = "source=EMP | eval LOG = log(2) | head 1 | fields LOG";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(LOG=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], LOG=[LOG(2, 2.718281828459045:DOUBLE)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "LOG=0.6931471805599453\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT LOG(2, 2.718281828459045) `LOG`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLog2WithOverriding() {
    String ppl = "source=EMP | eval LOG2 = log2(2) | head 1 | fields LOG2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(LOG2=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], LOG2=[LOG2(2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "LOG2=1.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT LOG2(2) `LOG2`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLog10WithOverriding() {
    String ppl = "source=EMP | eval LOG10 = log10(100) | head 1 | fields LOG10";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(LOG10=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], LOG10=[LOG10(100)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "LOG10=2.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT LOG10(100) `LOG10`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testModWithOverriding() {
    String ppl = "source=EMP | eval MOD = mod(10, 3) | head 2 | fields MOD";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(MOD=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], MOD=[MOD(10, 3)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "MOD=1.0\n" + "MOD=1.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "" + "SELECT MOD(10, 3) `MOD`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPiWithOverriding() {
    String ppl = "source=EMP | eval PI = pi() | head 1 | fields PI";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(PI=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], PI=[PI])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "PI=3.141592653589793\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "" + "SELECT PI `PI`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPowAndPowerWithOverriding() {
    String ppl = "source=EMP | eval POW = POW(3, 2), POWER = POWER(2, 2) | head 1 | fields POW, POWER";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(POW=[$8], POWER=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], POW=[POWER(3, 2)], POWER=[POWER(2, 2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "POW=9.0; POWER=4.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT POWER(3, 2) `POW`, POWER(2, 2) `POWER`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRadiansWithOverriding() {
    String ppl = "source=EMP | eval RADIANS = radians(180) | head 1 | fields RADIANS";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(RADIANS=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], RADIANS=[RADIANS(180)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "RADIANS=3.141592653589793\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT RADIANS(180) `RADIANS`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRandWithOverriding() {
    String ppl = "source=EMP | eval RAND = rand(1) | head 1 | fields RAND";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(RAND=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], RAND=[RAND(1)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql =
        "SELECT RAND(1) `RAND`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRoundWithOverriding() {
    String ppl = "source=EMP | eval ROUND = round(1.5) | head 1 | fields ROUND";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ROUND=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ROUND=[ROUND(1.5E0:DOUBLE)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ROUND=2.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT ROUND(1.5E0) `ROUND`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSignWithOverriding() {
    String ppl = "source=EMP | eval SIGN = sign(-1) | head 1 | fields SIGN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SIGN=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], SIGN=[SIGN(-1)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "SIGN=-1\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT SIGN(-1) `SIGN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSinWithOverriding() {
    String ppl = "source=EMP | eval SIN = sin(0) | head 1 | fields SIN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SIN=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], SIN=[SIN(0)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "SIN=0.0\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT SIN(0) `SIN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSqrtWithOverriding() {
    String ppl = "source=EMP | eval SQRT = sqrt(4) | head 2 | fields SQRT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
            "LogicalProject(SQRT=[$8])\n"
                    + "  LogicalSort(fetch=[2])\n"
                    + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
                    + " SAL=[$5], COMM=[$6], DEPTNO=[$7], SQRT=[SQRT(4)])\n"
                    + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "SQRT=2.0\n" + "SQRT=2.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT SQRT(4) `SQRT`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }


}

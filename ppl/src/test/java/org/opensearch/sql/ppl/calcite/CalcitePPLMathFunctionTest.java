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
    String ppl = "source=EMP | eval SAL = abs(-30) | fields SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL=[ABS(-30)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ABS(-30) `SAL`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAcos() {
    RelNode root = getRelNode("source=EMP | eval ACOS = acos(0) | fields ACOS");
    String expectedLogical =
        "LogicalProject(ACOS=[ACOS(0)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ACOS(0) `ACOS`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAsin() {
    RelNode root = getRelNode("source=EMP | eval ASIN = asin(0) |  fields ASIN");
    String expectedLogical =
        "LogicalProject(ASIN=[ASIN(0)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ASIN(0) `ASIN`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtan() {
    RelNode root = getRelNode("source=EMP | eval ATAN = atan(2) | fields ATAN");
    String expectedLogical =
        "LogicalProject(ATAN=[ATAN(2)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ATAN(2) `ATAN`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtan2() {
    RelNode root = getRelNode("source=EMP | eval ATAN2 = atan2(2, 3) | fields ATAN2");
    String expectedLogical =
        "LogicalProject(ATAN2=[ATAN2(2, 3)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ATAN2(2, 3) `ATAN2`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCbrt() {
    RelNode root = getRelNode("source=EMP | eval CBRT = cbrt(27) | fields CBRT");
    String expectedLogical =
        "LogicalProject(CBRT=[CBRT(27)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT CBRT(27) `CBRT`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCeiling() {
    RelNode root = getRelNode("source=EMP | eval CEILING = ceiling(4.5) | fields CEILING");
    String expectedLogical =
        "LogicalProject(CEILING=[CEIL(4.5E0:DOUBLE)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT CEIL(4.5E0) `CEILING`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConv() {
    RelNode root = getRelNode("source=EMP | eval CONV = conv(10, 10, 2) | fields CONV");
    String expectedLogical =
        "LogicalProject(CONV=[CONVERT(10, 10, 2)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT CONVERT(10, 10, 2) `CONV`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCos() {
    RelNode root = getRelNode("source=EMP | eval COS = cos(0) | fields COS");
    String expectedLogical =
        "LogicalProject(COS=[COS(0)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT COS(0) `COS`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCot() {
    RelNode root = getRelNode("source=EMP | eval COT = cot(1) | fields COT");
    String expectedLogical =
        "LogicalProject(COT=[COT(1)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT COT(1) `COT`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCrc32() {
    RelNode root = getRelNode("source=EMP | eval CRC32TEST = crc32('test') | fields CRC32TEST");
    String expectedLogical =
        "LogicalProject(CRC32TEST=[CRC32('test')])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT `CRC32`('test') `CRC32TEST`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDegrees() {
    RelNode root = getRelNode("source=EMP | eval DEGREES = degrees(1.57) | fields DEGREES");
    String expectedLogical =
        "LogicalProject(DEGREES=[DEGREES(1.57E0:DOUBLE)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT DEGREES(1.57E0) `DEGREES`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEuler() {
    RelNode root = getRelNode("source=EMP | eval EULER = e()| fields EULER");
    String expectedLogical =
        "LogicalProject(EULER=[E()])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT `E`() `EULER`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExp() {
    RelNode root = getRelNode("source=EMP | eval EXP = exp(2) | fields EXP");
    String expectedLogical =
        "LogicalProject(EXP=[EXP(2)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT EXP(2) `EXP`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFloor() {
    RelNode root = getRelNode("source=EMP | eval FLOOR1 = floor(50.00005) | fields FLOOR1");
    String expectedLogical =
        "LogicalProject(FLOOR1=[FLOOR(50.00005E0:DOUBLE)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT FLOOR(5.000005E1) `FLOOR1`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLn() {
    RelNode root = getRelNode("source=EMP | eval LN = ln(2) | fields LN");
    String expectedLogical =
        "LogicalProject(LN=[LN(2)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT LN(2) `LN`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLog() {
    RelNode root = getRelNode("source=EMP | eval LOG = log(2) | fields LOG");
    String expectedLogical =
        "LogicalProject(LOG=[LOG(2, 2.718281828459045E0:DOUBLE)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT LOG(2, 2.718281828459045E0) `LOG`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLog2() {
    RelNode root = getRelNode("source=EMP | eval LOG2 = log2(2) | fields LOG2");
    String expectedLogical =
        "LogicalProject(LOG2=[LOG2(2)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT LOG2(2) `LOG2`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLog10() {
    RelNode root = getRelNode("source=EMP | eval LOG10 = log10(100) | fields LOG10");
    String expectedLogical =
        "LogicalProject(LOG10=[LOG10(100)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT LOG10(100) `LOG10`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMod() {
    RelNode root = getRelNode("source=EMP | eval MOD = mod(10, 3) | fields MOD");
    String expectedLogical =
        "LogicalProject(MOD=[MOD(10, 3)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT MOD(10, 3) `MOD`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPi() {
    RelNode root = getRelNode("source=EMP | eval PI = pi() | fields PI");
    String expectedLogical = "LogicalProject(PI=[PI])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT PI `PI`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPowAndPower() {
    String ppl = "source=EMP | eval POW = POW(3, 2), POWER = POWER(2, 2) | fields POW, POWER";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(POW=[POWER(3, 2)], POWER=[POWER(2, 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT POWER(3, 2) `POW`, POWER(2, 2) `POWER`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRadians() {
    String ppl = "source=EMP | eval RADIANS = radians(180)  | fields RADIANS";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(RADIANS=[RADIANS(180)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT RADIANS(180) `RADIANS`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRand() {
    String ppl = "source=EMP | eval RAND = rand(1) | fields RAND";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(RAND=[RAND(1)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT RAND(1) `RAND`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRound() {
    String ppl = "source=EMP | eval ROUND = round(1.5) | fields ROUND";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ROUND=[ROUND(1.5E0:DOUBLE)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT ROUND(1.5E0) `ROUND`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSign() {
    String ppl = "source=EMP | eval SIGN = sign(-1) | fields SIGN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SIGN=[SIGN(-1)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT SIGN(-1) `SIGN`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSin() {
    RelNode root = getRelNode("source=EMP | eval SIN = sin(0) | fields SIN");
    String expectedLogical =
        "LogicalProject(SIN=[SIN(0)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT SIN(0) `SIN`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSqrt() {
    RelNode root = getRelNode("source=EMP | eval SQRT = sqrt(4) | fields SQRT");
    String expectedLogical =
        "LogicalProject(SQRT=[SQRT(4)])\n  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedSparkSql = "SELECT SQRT(4) `SQRT`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}

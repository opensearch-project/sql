/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    String ppl = "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(t, "LOWER function expects {[CHARACTER]}, but got [SHORT]");
  }

  @Test
  public void testTimeDiffWithUdtInputType() {
    String strPpl =
        "source=EMP | eval time_diff = timediff('12:00:00', '12:00:06') | fields time_diff";
    String timePpl =
        "source=EMP | eval time_diff = timediff(time('13:00:00'), time('12:00:06')) | fields"
            + " time_diff";
    String wrongPpl = "source=EMP | eval time_diff = timediff(12, '2009-12-10') | fields time_diff";
    getRelNode(strPpl);
    getRelNode(timePpl);
    Throwable t = Assert.assertThrows(Exception.class, () -> getRelNode(wrongPpl));
    verifyErrorMessageContains(
        t,
        "TIMEDIFF function expects {[STRING,STRING], [DATETIME,DATETIME], [DATETIME,STRING],"
            + " [STRING,DATETIME]}, but got [INTEGER,STRING]");
  }

  @Test
  public void testComparisonWithDifferentType() {
    getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    getRelNode("source=EMP | where ENAME <= 'Jack' | fields ENAME");
    String ppl = "source=EMP | where ENAME < 6 | fields ENAME";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t, "LESS function expects {[COMPARABLE_TYPE,COMPARABLE_TYPE]}, but got [STRING,INTEGER]");
  }

  @Test
  public void testCoalesceWithDifferentType() {
    String ppl =
        "source=EMP | eval coalesce_name = coalesce(EMPNO, 'Jack', ENAME) | fields"
            + " coalesce_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t, "COALESCE function expects {[COMPARABLE_TYPE...]}, but got [SHORT,STRING,STRING]");
  }

  @Test
  public void testSubstringWithWrongType() {
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1, 3) | fields sub_name");
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1) | fields sub_name");
    String ppl = "source=EMP | eval sub_name = substring(ENAME, 1, '3') | fields sub_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "SUBSTRING function expects {[STRING,INTEGER], [STRING,INTEGER,INTEGER]}, but got"
            + " [STRING,INTEGER,STRING]");
  }
}

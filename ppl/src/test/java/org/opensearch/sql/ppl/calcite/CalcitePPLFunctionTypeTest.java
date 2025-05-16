/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    String ppl = "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name";
    Assert.assertThrows(Exception.class, () -> getRelNode(ppl));
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
    Assert.assertThrows(Exception.class, () -> getRelNode(wrongPpl));
  }
}

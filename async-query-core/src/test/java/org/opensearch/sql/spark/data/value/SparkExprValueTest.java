/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.value;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.spark.data.type.SparkDataType;

class SparkExprValueTest {
  private final SparkDataType sparkDataType = new SparkDataType("char");

  @Test
  public void getters() {
    SparkExprValue sparkExprValue = new SparkExprValue(sparkDataType, "str");

    assertEquals(sparkDataType, sparkExprValue.type());
    assertEquals("str", sparkExprValue.value());
  }

  @Test
  public void unsupportedCompare() {
    SparkExprValue sparkExprValue = new SparkExprValue(sparkDataType, "str");

    assertThrows(UnsupportedOperationException.class, () -> sparkExprValue.compare(sparkExprValue));
  }

  @Test
  public void testEquals() {
    SparkExprValue sparkExprValue1 = new SparkExprValue(sparkDataType, "str");
    SparkExprValue sparkExprValue2 = new SparkExprValue(sparkDataType, "str");
    SparkExprValue sparkExprValue3 = new SparkExprValue(sparkDataType, "other");

    assertTrue(sparkExprValue1.equal(sparkExprValue2));
    assertFalse(sparkExprValue1.equal(sparkExprValue3));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.value;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.spark.data.type.SparkDataType;

class SparkExprValueTest {
  @Test
  public void type() {
    assertEquals(
        new SparkDataType("char"), new SparkExprValue(new SparkDataType("char"), "str").type());
  }

  @Test
  public void unsupportedCompare() {
    SparkDataType type = new SparkDataType("char");

    assertThrows(
        UnsupportedOperationException.class,
        () -> new SparkExprValue(type, "str").compare(new SparkExprValue(type, "str")));
  }
}

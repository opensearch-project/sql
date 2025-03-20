/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.type;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SparkDataTypeTest {

  @Test
  void testTypeName() {
    String expectedTypeName = "spark_string";
    SparkDataType sparkDataType = new SparkDataType(expectedTypeName);

    assertEquals(
        expectedTypeName, sparkDataType.typeName(), "Type name should match the expected value");
  }

  @Test
  void testEqualsAndHashCode() {
    SparkDataType type1 = new SparkDataType("spark_integer");
    SparkDataType type2 = new SparkDataType("spark_integer");
    SparkDataType type3 = new SparkDataType("spark_double");

    assertEquals(type1, type2);
    assertNotEquals(type1, type3);
    assertEquals(type1.hashCode(), type2.hashCode());
    assertNotEquals(type1.hashCode(), type3.hashCode());
  }
}

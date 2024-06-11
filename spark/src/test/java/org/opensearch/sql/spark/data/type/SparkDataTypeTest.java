/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SparkDataTypeTest {
  @Test
  public void testTypeName() {
    SparkDataType sparkDataType = new SparkDataType("TYPE_NAME");

    assertEquals("TYPE_NAME", sparkDataType.typeName());
  }
}

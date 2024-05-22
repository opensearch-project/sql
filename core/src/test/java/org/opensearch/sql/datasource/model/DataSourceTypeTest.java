/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class DataSourceTypeTest {
  @Test
  public void fromString_succeed() {
    testFromString("PROMETHEUS", DataSourceType.PROMETHEUS);
    testFromString("OPENSEARCH", DataSourceType.OPENSEARCH);
    testFromString("SPARK", DataSourceType.SPARK);
    testFromString("S3GLUE", DataSourceType.S3GLUE);

    testFromString("prometheus", DataSourceType.PROMETHEUS);
  }

  private void testFromString(String name, DataSourceType expectedType) {
    assertEquals(expectedType, DataSourceType.fromString(name));
  }

  @Test
  public void fromStringWithUnknownName_throws() {
    assertThrows(IllegalArgumentException.class, () -> DataSourceType.fromString("UnknownName"));
  }
}

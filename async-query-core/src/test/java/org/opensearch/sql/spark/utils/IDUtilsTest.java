/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IDUtilsTest {
  public static final String DATASOURCE_NAME = "DATASOURCE_NAME";

  @Test
  public void encodeAndDecode() {
    String id = IDUtils.encode(DATASOURCE_NAME);
    String decoded = IDUtils.decode(id);

    assertTrue(id.length() > IDUtils.PREFIX_LEN);
    assertEquals(DATASOURCE_NAME, decoded);
  }

  @Test
  public void generateUniqueIds() {
    String id1 = IDUtils.encode(DATASOURCE_NAME);
    String id2 = IDUtils.encode(DATASOURCE_NAME);

    assertNotEquals(id1, id2);
  }
}

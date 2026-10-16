/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class VectorDataTypeTest {

  @Test
  void fromMappingName() {
    assertEquals(VectorDataType.FLOAT, VectorDataType.fromMappingName("float"));
    assertEquals(VectorDataType.HALF_FLOAT, VectorDataType.fromMappingName("half_float"));
    assertEquals(VectorDataType.BYTE, VectorDataType.fromMappingName("BYTE"));
    assertEquals(VectorDataType.BINARY, VectorDataType.fromMappingName("binary"));
    assertEquals(VectorDataType.FLOAT, VectorDataType.fromMappingName(null));
    assertThrows(IllegalArgumentException.class, () -> VectorDataType.fromMappingName("int4"));
  }

  @Test
  void elementCount() {
    assertEquals(768, VectorDataType.FLOAT.elementCount(768));
    assertEquals(768, VectorDataType.BYTE.elementCount(768));
    assertEquals(96, VectorDataType.BINARY.elementCount(768));
    assertEquals(-1, VectorDataType.BINARY.elementCount(-1));
  }

  @Test
  void byteElements() {
    assertFalse(VectorDataType.FLOAT.hasByteElements());
    assertFalse(VectorDataType.HALF_FLOAT.hasByteElements());
    assertTrue(VectorDataType.BYTE.hasByteElements());
    assertTrue(VectorDataType.BINARY.hasByteElements());
    assertEquals("half_float", VectorDataType.HALF_FLOAT.getMappingName());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public class UtilsTest {

  @Test
  void testResolveNestedPathWithNullFieldTypes() {
    assertNull(Utils.resolveNestedPath("a.b.c", null));
  }

  @Test
  void testResolveNestedPathWithEmptyFieldTypes() {
    assertNull(Utils.resolveNestedPath("a.b.c", new HashMap<>()));
  }

  @Test
  void testResolveNestedPathWithArrayType() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.ARRAY);
    fieldTypes.put("a.b", ExprCoreType.STRING);

    assertEquals("a", Utils.resolveNestedPath("a.b.c", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithNestedArrayType() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.STRUCT);
    fieldTypes.put("a.b", ExprCoreType.ARRAY);
    fieldTypes.put("a.b.c", ExprCoreType.STRING);

    assertEquals("a.b", Utils.resolveNestedPath("a.b.c.d", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithNoArrayType() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.STRUCT);
    fieldTypes.put("a.b", ExprCoreType.STRUCT);
    fieldTypes.put("a.b.c", ExprCoreType.STRING);

    assertNull(Utils.resolveNestedPath("a.b.c.d", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithSingleLevel() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.STRING);

    assertNull(Utils.resolveNestedPath("a.b", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithTopLevelArray() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.ARRAY);

    assertEquals("a", Utils.resolveNestedPath("a.b", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithMultipleLevels() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("a", ExprCoreType.STRUCT);
    fieldTypes.put("a.b", ExprCoreType.STRUCT);
    fieldTypes.put("a.b.c", ExprCoreType.ARRAY);
    fieldTypes.put("a.b.c.d", ExprCoreType.STRING);

    assertEquals("a.b.c", Utils.resolveNestedPath("a.b.c.d.e", fieldTypes));
  }

  @Test
  void testResolveNestedPathWithNoParent() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("x", ExprCoreType.STRING);

    assertNull(Utils.resolveNestedPath("a", fieldTypes));
  }
}

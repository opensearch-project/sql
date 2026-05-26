/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class JsonExtractFunctionImplTest {

  @Test
  public void testMissingPathReturnsNull() {
    Object result = JsonExtractFunctionImpl.eval("{}", "name");
    assertNull(result, "Missing path should return actual null, not string \"null\"");
  }

  @Test
  public void testExplicitNullValueReturnsNull() {
    Object result = JsonExtractFunctionImpl.eval("{\"name\": null}", "name");
    assertNull(result, "Explicit JSON null value should return actual null, not string \"null\"");
  }

  @Test
  public void testNoArgsReturnsNull() {
    assertNull(JsonExtractFunctionImpl.eval());
  }

  @Test
  public void testSingleArgReturnsNull() {
    assertNull(JsonExtractFunctionImpl.eval("{}"));
  }

  @Test
  public void testExtractStringValue() {
    Object result = JsonExtractFunctionImpl.eval("{\"name\": \"John\"}", "name");
    assertEquals("John", result);
  }

  @Test
  public void testExtractNumericValue() {
    Object result = JsonExtractFunctionImpl.eval("{\"age\": 30}", "age");
    assertEquals("30", result);
  }

  @Test
  public void testExtractNestedObject() {
    Object result = JsonExtractFunctionImpl.eval("{\"user\": {\"name\": \"John\"}}", "user");
    assertEquals("{\"name\":\"John\"}", result);
  }

  @Test
  public void testExtractArray() {
    Object result = JsonExtractFunctionImpl.eval("{\"items\": [1, 2, 3]}", "items");
    assertEquals("[1,2,3]", result);
  }

  @Test
  public void testExtractBooleanValue() {
    Object result = JsonExtractFunctionImpl.eval("{\"active\": true}", "active");
    assertEquals("true", result);
  }

  @Test
  public void testMultiPathWithMissingPath() {
    Object result = JsonExtractFunctionImpl.eval("{\"name\": \"John\"}", "name", "age");
    assertEquals("[\"John\",null]", result);
  }

  @Test
  public void testMultiPathAllMissing() {
    Object result = JsonExtractFunctionImpl.eval("{}", "name", "age");
    assertEquals("[null,null]", result);
  }
}

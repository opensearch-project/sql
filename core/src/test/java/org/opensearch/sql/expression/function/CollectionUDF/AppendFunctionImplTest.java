/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AppendFunctionImplTest {

  @Test
  public void testAppendWithNoArguments() {
    Object result = AppendFunctionImpl.append();
    assertNull(result);
  }

  @Test
  public void testAppendWithSingleElement() {
    Object result = AppendFunctionImpl.append(42);
    assertEquals(42, result);
  }

  @Test
  public void testAppendWithMultipleElements() {
    Object result = AppendFunctionImpl.append(1, 2, 3);
    assertEquals(Arrays.asList(1, 2, 3), result);
  }

  @Test
  public void testAppendWithNullValues() {
    Object result = AppendFunctionImpl.append(null, 1, null);
    assertEquals(1, result);
  }

  @Test
  public void testAppendWithAllNulls() {
    Object result = AppendFunctionImpl.append(null, null);
    assertNull(result);
  }

  @Test
  public void testAppendWithArrayFlattening() {
    List<Integer> array1 = Arrays.asList(1, 2);
    List<Integer> array2 = Arrays.asList(3, 4);
    Object result = AppendFunctionImpl.append(array1, array2);
    assertEquals(Arrays.asList(1, 2, 3, 4), result);
  }

  @Test
  public void testAppendWithMixedTypes() {
    List<Integer> array = Arrays.asList(1, 2);
    Object result = AppendFunctionImpl.append(array, 3, "hello");
    assertEquals(Arrays.asList(1, 2, 3, "hello"), result);
  }

  @Test
  public void testAppendWithArrayAndNulls() {
    List<Integer> array = Arrays.asList(1, 2);
    Object result = AppendFunctionImpl.append(null, array, null, 3);
    assertEquals(Arrays.asList(1, 2, 3), result);
  }

  @Test
  public void testAppendWithSingleNull() {
    Object result = AppendFunctionImpl.append((Object) null);
    assertNull(result);
  }

  @Test
  public void testAppendWithEmptyArray() {
    List<Object> emptyArray = Arrays.asList();
    Object result = AppendFunctionImpl.append(emptyArray, 1);
    assertEquals(1, result);
  }
}

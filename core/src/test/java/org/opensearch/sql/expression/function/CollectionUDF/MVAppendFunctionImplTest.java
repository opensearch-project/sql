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

/** Unit tests for MVAppendFunctionImpl. */
public class MVAppendFunctionImplTest {

  @Test
  public void testMvappendWithNoArguments() {
    Object result = MVAppendFunctionImpl.mvappend();
    assertNull(result);
  }

  @Test
  public void testMvappendWithSingleElement() {
    Object result = MVAppendFunctionImpl.mvappend(42);
    assertEquals(Arrays.asList(42), result);
  }

  @Test
  public void testMvappendWithMultipleElements() {
    Object result = MVAppendFunctionImpl.mvappend(1, 2, 3);
    assertEquals(Arrays.asList(1, 2, 3), result);
  }

  @Test
  public void testMvappendWithNullValues() {
    Object result = MVAppendFunctionImpl.mvappend(null, 1, null);
    assertEquals(Arrays.asList(1), result);
  }

  @Test
  public void testMvappendWithAllNulls() {
    Object result = MVAppendFunctionImpl.mvappend(null, null);
    assertNull(result);
  }

  @Test
  public void testMvappendWithArrayFlattening() {
    List<Integer> array1 = Arrays.asList(1, 2);
    List<Integer> array2 = Arrays.asList(3, 4);
    Object result = MVAppendFunctionImpl.mvappend(array1, array2);
    assertEquals(Arrays.asList(1, 2, 3, 4), result);
  }

  @Test
  public void testMvappendWithMixedTypes() {
    List<Integer> array = Arrays.asList(1, 2);
    Object result = MVAppendFunctionImpl.mvappend(array, 3, "hello");
    assertEquals(Arrays.asList(1, 2, 3, "hello"), result);
  }

  @Test
  public void testMvappendWithArrayAndNulls() {
    List<Integer> array = Arrays.asList(1, 2);
    Object result = MVAppendFunctionImpl.mvappend(null, array, null, 3);
    assertEquals(Arrays.asList(1, 2, 3), result);
  }

  @Test
  public void testMvappendWithSingleNull() {
    Object result = MVAppendFunctionImpl.mvappend((Object) null);
    assertNull(result);
  }

  @Test
  public void testMvappendWithEmptyArray() {
    List<Object> emptyArray = Arrays.asList();
    Object result = MVAppendFunctionImpl.mvappend(emptyArray, 1);
    assertEquals(Arrays.asList(1), result);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.jsonUDF.ForeachJsonArrayFunctionImpl;

public class ForeachFunctionImplTest {

  @Test
  public void testPairCollectionPreservesNullElements() {
    List<?> pairs =
        (List<?>)
            ForeachPairCollectionFunctionImpl.eval(
                new Object[] {new Object[] {1, null, 3}, "captured"});

    assertEquals(3, pairs.size());
    assertArrayEquals(new Object[] {1, 0, "captured"}, (Object[]) pairs.get(0));
    assertArrayEquals(new Object[] {null, 1, "captured"}, (Object[]) pairs.get(1));
    assertArrayEquals(new Object[] {3, 2, "captured"}, (Object[]) pairs.get(2));
  }

  @Test
  public void testJsonArrayPreservesTopLevelNestedValuesAndNull() {
    Object values = ForeachJsonArrayFunctionImpl.eval("[[1,2],{\"a\":1},null]", "VARCHAR");

    assertEquals(List.of("[1,2]", "{\"a\":1}", "null"), values);
  }

  @Test
  public void testMalformedJsonArrayIsEmpty() {
    assertEquals(List.of(), ForeachJsonArrayFunctionImpl.eval("not-json", "VARCHAR"));
  }

  @Test
  public void testJsonArraySafelyCoercesNumericElements() {
    assertEquals(
        Arrays.asList(10.0, 20.0, null),
        ForeachJsonArrayFunctionImpl.eval("[10,\"20\",\"not-a-number\"]", "DOUBLE"));
  }

  @Test
  public void testStatePreservesHeterogeneousAndNullSlots() {
    assertEquals(
        Arrays.asList(3, null, "seen"),
        ForeachStateFunctionImpl.eval(new Object[] {3, null, "seen"}));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class MixedTypeComparatorTest {

  private final MixedTypeComparator comparator = MixedTypeComparator.INSTANCE;

  @Test
  public void testNumericComparison() {
    assertEquals(-1, comparator.compare(1, 2));
    assertEquals(1, comparator.compare(2, 1));
    assertEquals(0, comparator.compare(5, 5));

    // Different numeric types
    assertEquals(-1, comparator.compare(1, 2.5));
    assertEquals(1, comparator.compare(3.14, 2));
    assertEquals(0, comparator.compare(4, 4.0));
    assertEquals(-1, comparator.compare(10L, new BigDecimal("20")));
  }

  @Test
  public void testStringComparison() {
    assertEquals(-1, comparator.compare("apple", "banana"));
    assertEquals(1, comparator.compare("zebra", "apple"));
    assertEquals(0, comparator.compare("test", "test"));
    assertEquals(-1, comparator.compare("ABC", "abc")); //
    assertEquals(1, comparator.compare("hello", "HELLO"));
  }

  @Test
  public void testMixedTypeComparison() {
    assertEquals(-1, comparator.compare(42, "apple"));
    assertEquals(1, comparator.compare("apple", 42));
    assertEquals(-1, comparator.compare(3.14, "hello"));
    assertEquals(1, comparator.compare("world", 100L));
    assertEquals(-1, comparator.compare(0, "0"));
    assertEquals(1, comparator.compare("123", 456));
  }
}

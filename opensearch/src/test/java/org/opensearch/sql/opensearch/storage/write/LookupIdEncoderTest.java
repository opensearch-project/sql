/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class LookupIdEncoderTest {

  private static final List<String> FIELDS = List.of("a", "b");

  private String id(Object... row) {
    return LookupIdEncoder.encode(List.of("a", "b"), FIELDS, row);
  }

  @Test
  void deterministicAndFixedLength() {
    String first = id("alice", 42);
    assertEquals(first, id("alice", 42), "same key -> same id (idempotent upsert)");
    assertEquals(43, first.length(), "base64url SHA-256 is a bounded 43-char id");
  }

  @Test
  void multiFieldNoCollisionAcrossBoundaries() {
    // length prefixes keep field boundaries distinct so concatenations cannot collide
    assertNotEquals(
        LookupIdEncoder.encode(List.of("a", "b"), FIELDS, new Object[] {"a", "bc"}),
        LookupIdEncoder.encode(List.of("a", "b"), FIELDS, new Object[] {"ab", "c"}));
  }

  @Test
  void emptyDistinctFromNullAndMissingCollapsed() {
    String nullVal = LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {null});
    String emptyVal = LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {""});
    String missing = LookupIdEncoder.encode(List.of("a"), List.of("x"), new Object[] {"v"});
    assertNotEquals(nullVal, emptyVal, "null must differ from empty string");
    assertNotEquals(emptyVal, missing, "empty string must differ from a missing field");
    assertEquals(nullVal, missing, "null and missing both encode as N");
  }

  @Test
  void typedValuesDoNotCollideWithStrings() {
    assertNotEquals(
        LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {1}),
        LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {"1"}));
  }

  @Test
  void multivalueKeyRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {new Object[] {1, 2}}));
    assertThrows(
        IllegalArgumentException.class,
        () -> LookupIdEncoder.encode(List.of("a"), List.of("a"), new Object[] {List.of(1, 2)}));
  }
}

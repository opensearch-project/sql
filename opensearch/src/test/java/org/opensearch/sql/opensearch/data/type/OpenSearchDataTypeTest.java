/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.junit.jupiter.api.Test;

class OpenSearchDataTypeTest {
  @Test
  public void testIsCompatible() {
    assertTrue(STRING.isCompatible(OPENSEARCH_TEXT));
    assertFalse(OPENSEARCH_TEXT.isCompatible(STRING));

    assertTrue(STRING.isCompatible(OPENSEARCH_TEXT_KEYWORD));
    assertTrue(OPENSEARCH_TEXT.isCompatible(OPENSEARCH_TEXT_KEYWORD));
  }

  @Test
  public void testTypeName() {
    assertEquals("string", OPENSEARCH_TEXT.typeName());
    assertEquals("string", OPENSEARCH_TEXT_KEYWORD.typeName());
  }

  @Test
  public void legacyTypeName() {
    assertEquals("text", OPENSEARCH_TEXT.legacyTypeName());
    assertEquals("text", OPENSEARCH_TEXT_KEYWORD.legacyTypeName());
  }

  @Test
  public void testShouldCast() {
    assertFalse(OPENSEARCH_TEXT.shouldCast(STRING));
    assertFalse(OPENSEARCH_TEXT_KEYWORD.shouldCast(STRING));
  }
}

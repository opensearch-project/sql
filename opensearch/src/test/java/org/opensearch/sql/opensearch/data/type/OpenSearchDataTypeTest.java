/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
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

  @Test
  public void testGetExprType() {
    assertEquals(OPENSEARCH_TEXT, OpenSearchDataType.getExprType("text"));
    assertEquals(FLOAT, OpenSearchDataType.getExprType("float"));
    assertEquals(FLOAT, OpenSearchDataType.getExprType("half_float"));
    assertEquals(DOUBLE, OpenSearchDataType.getExprType("double"));
    assertEquals(DOUBLE, OpenSearchDataType.getExprType("scaled_float"));
    assertEquals(TIMESTAMP, OpenSearchDataType.getExprType("date"));
    assertEquals(TIMESTAMP, OpenSearchDataType.getExprType("date_nanos"));
  }

  @Test
  public void testGetOpenSearchType() {
    assertEquals("text", OpenSearchDataType.getOpenSearchType(OPENSEARCH_TEXT));
    assertEquals("float", OpenSearchDataType.getOpenSearchType(FLOAT));
    assertEquals("double", OpenSearchDataType.getOpenSearchType(DOUBLE));
    assertEquals("date", OpenSearchDataType.getOpenSearchType(TIMESTAMP));
  }
}

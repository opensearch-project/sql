/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.lang;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;

class PPLLangSpecTest {
  /** Tests that the language type of the PPL specification is PPL. */
  @Test
  public void testPPLSpecLanguage() {
    PPLLangSpec spec = PPL_SPEC;
    assertEquals(LangSpec.LangType.PPL, spec.language(), "Expected language to be PPL.");
  }

  /**
   * Tests that the PPL specification returns the correct type name mapping for known expression
   * types. Assumes that ExprCoreType constants are available.
   */
  @Test
  public void testPPLSpecTypeNameMapping() {
    PPLLangSpec spec = PPL_SPEC;
    assertEquals("tinyint", spec.typeName(ExprCoreType.BYTE), "BYTE should map to tinyint.");
    assertEquals("smallint", spec.typeName(ExprCoreType.SHORT), "SHORT should map to smallint.");
    assertEquals("int", spec.typeName(ExprCoreType.INTEGER), "INTEGER should map to int.");
    assertEquals("bigint", spec.typeName(ExprCoreType.LONG), "LONG should map to bigint.");
  }

  /**
   * Tests that an unmapped expression type returns its default type name in the PPL specification.
   */
  @Test
  public void testPPLSpecDefaultTypeName() {
    String result = PPL_SPEC.typeName(ExprCoreType.STRING);
    assertEquals("STRING", result, "Unmapped expression type should return its default type name.");
  }
}

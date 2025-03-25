/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.lang;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.QueryType;

class LangSpecTest {
  @Test
  public void testFromLanguageSQL() {
    LangSpec spec = LangSpec.fromLanguage("SQL");
    assertEquals(QueryType.SQL, spec.language(), "Expected language type to be SQL");
    assertSame(LangSpec.SQL_SPEC, spec, "Expected SQL_SPEC instance for SQL language.");
  }

  @Test
  public void testSQLSpecDefaultTypeName() {
    String result = LangSpec.SQL_SPEC.typeName(ExprCoreType.BYTE);
    assertEquals("BYTE", result, "SQL_SPEC should return the expression type's default type name.");
  }
}

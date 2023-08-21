/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.LONG;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.function.ScalarFunction.LOG;

import org.junit.Test;

/** Generic type test */
public class GenericTypeTest {

  @Test
  public void passNumberArgToLogShouldReturnNumber() {
    assertEquals(DOUBLE, LOG.construct(singletonList(NUMBER)));
  }

  @Test
  public void passIntegerArgToLogShouldReturnDouble() {
    assertEquals(DOUBLE, LOG.construct(singletonList(INTEGER)));
  }

  @Test
  public void passLongArgToLogShouldReturnDouble() {
    assertEquals(DOUBLE, LOG.construct(singletonList(LONG)));
  }

  @Test
  public void passTextArgToLogShouldReturnTypeError() {
    assertEquals(TYPE_ERROR, LOG.construct(singletonList(TEXT)));
  }

  @Test
  public void passKeywordArgToLogShouldReturnTypeError() {
    assertEquals(TYPE_ERROR, LOG.construct(singletonList(KEYWORD)));
  }
}

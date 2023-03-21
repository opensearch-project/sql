/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.pagination.Cursor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CursorTest {

  @Test
  void empty_array_is_none() {
    Assertions.assertEquals(Cursor.None, new Cursor(null));
  }

  @Test
  void toString_is_array_value() {
    String cursorTxt = "This is a test";
    Assertions.assertEquals(cursorTxt, new Cursor(cursorTxt).toString());
  }
}

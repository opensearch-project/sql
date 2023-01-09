/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CursorTest {

  @Test
  void EmptyArrayIsNone() {
    Assertions.assertEquals(Cursor.None, new Cursor(new byte[]{}));
  }

  @Test
  void ToStringIsArrayValue() {
    String cursorTxt = "This is a test";
    Assertions.assertEquals(cursorTxt, new Cursor(cursorTxt.getBytes()).toString());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.cursor;

public interface Cursor {

  NullCursor NULL_CURSOR = new NullCursor();

  /**
   * All cursor's are of the form <cursorType>:<base64 encoded cursor><br>
   * The serialized form before encoding is upto Cursor implementation
   */
  String generateCursorId();

  CursorType getType();
}

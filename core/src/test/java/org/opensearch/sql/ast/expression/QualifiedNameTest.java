/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QualifiedNameTest {

  @Test
  void can_return_prefix_and_suffix() {
    QualifiedName name = QualifiedName.of("first", "second", "third");
    assertTrue(name.getPrefix().isPresent());
    assertEquals(QualifiedName.of("first", "second"), name.getPrefix().get());
    assertEquals("third", name.getSuffix());
  }

  @Test
  void can_return_first_and_rest() {
    QualifiedName name = QualifiedName.of("first", "second", "third");
    assertTrue(name.first().isPresent());
    assertEquals("first", name.first().get());
    assertEquals(QualifiedName.of("second", "third"), name.rest());
  }

  @Test
  void should_return_empty_if_only_single_part() {
    QualifiedName name = QualifiedName.of("test");
    assertFalse(name.first().isPresent());
    assertFalse(name.getPrefix().isPresent());
  }
}

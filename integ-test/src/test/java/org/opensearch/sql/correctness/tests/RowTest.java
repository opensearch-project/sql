/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.opensearch.sql.correctness.runner.resultset.Row;

/** Unit test {@link Row} */
public class RowTest {

  @Test
  public void rowShouldEqualToOtherRowWithSimilarFloat() {
    Row row1 = new Row();
    Row row2 = new Row();
    row1.add(1.000001);
    row2.add(1.000002);
    assertEquals(row1, row2);
    assertEquals(row2, row1);
  }

  @Test
  public void rowShouldNotEqualToOtherRowWithDifferentString() {
    Row row1 = new Row();
    Row row2 = new Row();
    row1.add("hello");
    row2.add("hello1");
    assertNotEquals(row1, row2);
    assertNotEquals(row2, row1);
  }

  @Test
  public void shouldConsiderNullGreater() {
    Row row1 = new Row();
    Row row2 = new Row();
    row1.add("hello");
    row1.add(null);
    row2.add("hello");
    row2.add("world");
    assertEquals(1, row1.compareTo(row2));
  }
}

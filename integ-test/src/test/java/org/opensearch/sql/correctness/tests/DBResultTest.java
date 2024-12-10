/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import org.junit.Test;
import org.opensearch.sql.correctness.runner.resultset.DBResult;
import org.opensearch.sql.correctness.runner.resultset.Row;
import org.opensearch.sql.correctness.runner.resultset.Type;

/** Unit tests for {@link DBResult} */
public class DBResultTest {

  @Test
  public void dbResultFromDifferentDbNameShouldEqual() {
    DBResult result1 = new DBResult("DB 1", List.of(new Type("name", "VARCHAR")), List.of());
    DBResult result2 = new DBResult("DB 2", List.of(new Type("name", "VARCHAR")), List.of());
    assertEquals(result1, result2);
  }

  @Test
  public void dbResultWithDifferentColumnShouldNotEqual() {
    DBResult result1 = new DBResult("DB 1", List.of(new Type("name", "VARCHAR")), List.of());
    DBResult result2 = new DBResult("DB 2", List.of(new Type("age", "INT")), List.of());
    assertNotEquals(result1, result2);
  }

  @Test
  public void dbResultWithSameRowsInDifferentOrderShouldEqual() {
    DBResult result1 = DBResult.result("DB 1");
    result1.addColumn("name", "VARCHAR");
    result1.addRow(new Row(ImmutableList.of("test-1")));
    result1.addRow(new Row(ImmutableList.of("test-2")));

    DBResult result2 = DBResult.result("DB 2");
    result2.addColumn("name", "VARCHAR");
    result2.addRow(new Row(ImmutableList.of("test-2")));
    result2.addRow(new Row(ImmutableList.of("test-1")));

    assertEquals(result1, result2);
  }

  @Test
  public void dbResultInOrderWithSameRowsInDifferentOrderShouldNotEqual() {
    DBResult result1 = DBResult.resultInOrder("DB 1");
    result1.addColumn("name", "VARCHAR");
    result1.addRow(new Row(ImmutableList.of("test-1")));
    result1.addRow(new Row(ImmutableList.of("test-2")));

    DBResult result2 = DBResult.resultInOrder("DB 2");
    result2.addColumn("name", "VARCHAR");
    result2.addRow(new Row(ImmutableList.of("test-2")));
    result2.addRow(new Row(ImmutableList.of("test-1")));

    assertNotEquals(result1, result2);
  }

  @Test
  public void dbResultWithDifferentColumnTypeShouldNotEqual() {
    DBResult result1 = new DBResult("DB 1", List.of(new Type("age", "FLOAT")), List.of());
    DBResult result2 = new DBResult("DB 2", List.of(new Type("age", "INT")), List.of());
    assertNotEquals(result1, result2);
  }

  @Test
  public void shouldExplainColumnTypeDifference() {
    DBResult result1 =
        new DBResult(
            "DB 1", List.of(new Type("name", "VARCHAR"), new Type("age", "FLOAT")), List.of());
    DBResult result2 =
        new DBResult(
            "DB 2", List.of(new Type("name", "VARCHAR"), new Type("age", "INT")), List.of());

    assertEquals(
        "Schema type at [1] is different: "
            + "this=[Type(name=age, type=FLOAT)], other=[Type(name=age, type=INT)]",
        result1.diff(result2));
  }

  @Test
  public void shouldExplainDataRowsDifference() {
    DBResult result1 =
        new DBResult(
            "DB 1",
            List.of(new Type("name", "VARCHAR")),
            Sets.newHashSet(
                new Row(List.of("hello")),
                new Row(List.of("world")),
                new Row(Lists.newArrayList((Object) null))));
    DBResult result2 =
        new DBResult(
            "DB 2",
            List.of(new Type("name", "VARCHAR")),
            Sets.newHashSet(
                new Row(Lists.newArrayList((Object) null)),
                new Row(List.of("hello")),
                new Row(List.of("world123"))));

    assertEquals(
        "Data row at [1] is different: this=[Row(values=[world])], other=[Row(values=[world123])]",
        result1.diff(result2));
  }

  @Test
  public void shouldExplainDataRowsOrderDifference() {
    DBResult result1 = DBResult.resultInOrder("DB 1");
    result1.addColumn("name", "VARCHAR");
    result1.addRow(new Row(ImmutableList.of("hello")));
    result1.addRow(new Row(ImmutableList.of("world")));

    DBResult result2 = DBResult.resultInOrder("DB 2");
    result2.addColumn("name", "VARCHAR");
    result2.addRow(new Row(ImmutableList.of("world")));
    result2.addRow(new Row(ImmutableList.of("hello")));

    assertEquals(
        "Data row at [0] is different: this=[Row(values=[hello])], other=[Row(values=[world])]",
        result1.diff(result2));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;

class QueryResultTest {

  private ExecutionEngine.Schema schema = new ExecutionEngine.Schema(ImmutableList.of(
      new ExecutionEngine.Schema.Column("name", null, STRING),
      new ExecutionEngine.Schema.Column("age", null, INTEGER)));


  @Test
  void size() {
    QueryResult response = new QueryResult(
        schema,
        Arrays.asList(
            tupleValue(ImmutableMap.of("name", "John", "age", 20)),
            tupleValue(ImmutableMap.of("name", "Allen", "age", 30)),
            tupleValue(ImmutableMap.of("name", "Smith", "age", 40))
        ), Cursor.None);
    assertEquals(3, response.size());
  }

  @Test
  void columnNameTypes() {
    QueryResult response = new QueryResult(
        schema,
        Collections.singletonList(
            tupleValue(ImmutableMap.of("name", "John", "age", 20))
        ), Cursor.None);

    assertEquals(
        ImmutableMap.of("name", "string", "age", "integer"),
        response.columnNameTypes()
    );
  }

  @Test
  void columnNameTypesWithAlias() {
    ExecutionEngine.Schema schema = new ExecutionEngine.Schema(ImmutableList.of(
        new ExecutionEngine.Schema.Column("name", "n", STRING)));
    QueryResult response = new QueryResult(
        schema,
        Collections.singletonList(tupleValue(ImmutableMap.of("n", "John"))),
        Cursor.None);

    assertEquals(
        ImmutableMap.of("n", "string"),
        response.columnNameTypes()
    );
  }

  @Test
  void columnNameTypesFromEmptyExprValues() {
    QueryResult response = new QueryResult(
        schema,
        Collections.emptyList(), Cursor.None);
    assertEquals(
        ImmutableMap.of("name", "string", "age", "integer"),
        response.columnNameTypes()
    );
  }

  @Test
  void columnNameTypesFromExprValuesWithMissing() {
    QueryResult response = new QueryResult(
        schema,
        Arrays.asList(
            tupleValue(ImmutableMap.of("name", "John")),
            tupleValue(ImmutableMap.of("name", "John", "age", 20))
        ));

    assertEquals(
        ImmutableMap.of("name", "string", "age", "integer"),
        response.columnNameTypes()
    );
  }

  @Test
  void iterate() {
    QueryResult response = new QueryResult(
        schema,
        Arrays.asList(
            tupleValue(ImmutableMap.of("name", "John", "age", 20)),
            tupleValue(ImmutableMap.of("name", "Allen", "age", 30))
        ), Cursor.None);

    int i = 0;
    for (Object[] objects : response) {
      if (i == 0) {
        assertArrayEquals(new Object[] {"John", 20}, objects);
      } else if (i == 1) {
        assertArrayEquals(new Object[] {"Allen", 30}, objects);
      } else {
        fail("More rows returned than expected");
      }
      i++;
    }
  }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.QueryResult;

class SimpleJsonResponseFormatterTest {

  private final ExecutionEngine.Schema schema =
      new ExecutionEngine.Schema(
          ImmutableList.of(
              new ExecutionEngine.Schema.Column("firstname", null, STRING),
              new ExecutionEngine.Schema.Column("age", null, INTEGER)));

  @Test
  void formatResponse() {
    QueryResult response =
        new QueryResult(
            schema,
            List.of(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatResponsePretty() {
    QueryResult response =
        new QueryResult(
            schema,
            List.of(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Smith\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        formatter.format(response));
  }

  @Test
  void formatResponseSchemaWithAlias() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(new ExecutionEngine.Schema.Column("firstname", "name", STRING)));
    QueryResult response =
        new QueryResult(
            schema, ImmutableList.of(tupleValue(ImmutableMap.of("name", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"name\",\"type\":\"string\"}],"
            + "\"datarows\":[[\"John\",20]],\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithMissingValue() {
    QueryResult response =
        new QueryResult(
            schema,
            List.of(
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("firstname", stringValue("John"), "age", LITERAL_MISSING)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"John\",null],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithTupleValue() {
    QueryResult response =
        new QueryResult(
            schema,
            List.of(
                tupleValue(
                    ImmutableMap.of(
                        "name",
                        "Smith",
                        "address",
                        ImmutableMap.of(
                            "state", "WA", "street", ImmutableMap.of("city", "seattle"))))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);

    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"Smith\",{\"state\":\"WA\",\"street\":{\"city\":\"seattle\"}}]],"
            + "\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithArrayValue() {
    QueryResult response =
        new QueryResult(
            schema,
            List.of(
                tupleValue(
                    ImmutableMap.of(
                        "name",
                        "Smith",
                        "address",
                        List.of(
                            ImmutableMap.of("state", "WA"), ImmutableMap.of("state", "NYC"))))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"Smith\",[{\"state\":\"WA\"},{\"state\":\"NYC\"}]]],"
            + "\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatError() {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"type\":\"RuntimeException\",\"reason\":\"This is an exception\"}",
        formatter.format(new RuntimeException("This is an exception")));
  }

  @Test
  void formatErrorPretty() {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    assertEquals(
        "{\n"
            + "  \"type\": \"RuntimeException\",\n"
            + "  \"reason\": \"This is an exception\"\n"
            + "}",
        formatter.format(new RuntimeException("This is an exception")));
  }
}

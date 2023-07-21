/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.CONTENT_TYPE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.QueryResult;

public class VisualizationResponseFormatterTest {
  private final VisualizationResponseFormatter formatter = new VisualizationResponseFormatter(
      JsonResponseFormatter.Style.COMPACT);

  @Test
  void formatResponse() {
    QueryResult response = new QueryResult(
        new ExecutionEngine.Schema(ImmutableList.of(
            new ExecutionEngine.Schema.Column("name", "name", STRING),
            new ExecutionEngine.Schema.Column("age", "age", INTEGER))),
        ImmutableList.of(tupleValue(ImmutableMap.of("name", "John", "age", 20)),
            tupleValue(ImmutableMap.of("name", "Amy", "age", 31)),
            tupleValue(ImmutableMap.of("name", "Bob", "age", 28))));

    assertJsonEquals(
        "{\"data\":{"
            + "\"name\":[\"John\",\"Amy\",\"Bob\"],"
            + "\"age\":[20,31,28]},"
            + "\"metadata\":{"
            + "\"fields\":["
            + "{\"name\":\"name\",\"type\":\"keyword\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}]},"
            + "\"size\":3,"
            + "\"status\":200"
            + "}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithNull() {
    QueryResult response =
        new QueryResult(
            new ExecutionEngine.Schema(ImmutableList.of(
                new ExecutionEngine.Schema.Column("name", null, STRING),
                new ExecutionEngine.Schema.Column("age", null, INTEGER))),
            ImmutableList.of(tupleValue(ImmutableMap.of("name", "John", "age", LITERAL_MISSING)),
                tupleValue(ImmutableMap.of("name", "Allen", "age", LITERAL_NULL)),
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));

    assertJsonEquals(
        "{\"data\":{"
            + "\"name\":[\"John\",\"Allen\",\"Smith\"],"
            + "\"age\":[null,null,30]},"
            + "\"metadata\":{"
            + "\"fields\":["
            + "{\"name\":\"name\",\"type\":\"keyword\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}]},"
            + "\"size\":3,"
            + "\"status\":200"
            + "}",
        formatter.format(response)
    );
  }

  @Test
  void clientErrorSyntaxException() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"SyntaxCheckException\","
            + "\"reason\":\"Invalid Query\","
            + "\"details\":\"Invalid query syntax\""
            + "},"
            + "\"status\":400}",
        formatter.format(new SyntaxCheckException("Invalid query syntax"))
    );
  }

  @Test
  void clientErrorSemanticException() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"SemanticCheckException\","
            + "\"reason\":\"Invalid Query\","
            + "\"details\":\"Invalid query semantics\""
            + "},"
            + "\"status\":400}",
        formatter.format(new SemanticCheckException("Invalid query semantics"))
    );
  }

  @Test
  void serverError() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"IllegalStateException\","
            + "\"reason\":\"There was internal problem at backend\","
            + "\"details\":\"Execution error\""
            + "},"
            + "\"status\":503}",
        formatter.format(new IllegalStateException("Execution error"))
    );
  }

  @Test
  void opensearchServerError() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"OpenSearchException\","
            + "\"reason\":\"Error occurred in OpenSearch engine: all shards failed\","
            + "\"details\":\"OpenSearchException[all shards failed]; "
            + "nested: IllegalStateException[Execution error];; "
            + "java.lang.IllegalStateException: Execution error\\n"
            + "For more details, please send request for Json format to see the raw response "
            + "from OpenSearch engine.\""
            + "},"
            + "\"status\":503}",
        formatter.format(new OpenSearchException("all shards failed",
            new IllegalStateException("Execution error")))
    );
  }

  @Test
  void prettyStyle() {
    VisualizationResponseFormatter prettyFormatter = new VisualizationResponseFormatter(
        JsonResponseFormatter.Style.PRETTY);
    QueryResult response = new QueryResult(
        new ExecutionEngine.Schema(ImmutableList.of(
            new ExecutionEngine.Schema.Column("name", "name", STRING),
            new ExecutionEngine.Schema.Column("age", "age", INTEGER))),
        ImmutableList.of(tupleValue(ImmutableMap.of("name", "John", "age", 20)),
            tupleValue(ImmutableMap.of("name", "Amy", "age", 31)),
            tupleValue(ImmutableMap.of("name", "Bob", "age", 28))));

    assertJsonEquals(
        "{\n"
            + "  \"data\": {\n"
            + "    \"name\": [\n"
            + "      \"John\",\n"
            + "      \"Amy\",\n"
            + "      \"Bob\"\n"
            + "    ],\n"
            + "    \"age\": [\n"
            + "      20,\n"
            + "      31,\n"
            + "      28\n"
            + "    ]\n"
            + "  },\n"
            + "  \"metadata\": {\n"
            + "    \"fields\": [\n"
            + "      {\n"
            + "        \"name\": \"name\",\n"
            + "        \"type\": \"keyword\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"age\",\n"
            + "        \"type\": \"integer\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"size\": 3,\n"
            + "  \"status\": 200\n"
            + "}",
        prettyFormatter.format(response)
    );
  }

  private static void assertJsonEquals(String expected, String actual) {
    assertEquals(
        JsonParser.parseString(expected),
        JsonParser.parseString(actual));
  }

  @Test
  void testContentType() {
    var formatter = new CommandResponseFormatter();
    assertEquals(formatter.contentType(), CONTENT_TYPE);
  }
}

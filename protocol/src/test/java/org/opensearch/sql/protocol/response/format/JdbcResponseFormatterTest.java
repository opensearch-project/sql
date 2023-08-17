/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.executor.ExecutionEngine.Schema;
import static org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.protocol.response.QueryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class JdbcResponseFormatterTest {

  private final JdbcResponseFormatter formatter = new JdbcResponseFormatter(COMPACT);

  @Test
  void format_response() {
    QueryResult response =
        new QueryResult(
            new Schema(
                ImmutableList.of(
                    new Column("name", "name", STRING),
                    new Column("address1", "address1", OpenSearchTextType.of()),
                    new Column(
                        "address2",
                        "address2",
                        OpenSearchTextType.of(
                            Map.of(
                                "words",
                                OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))),
                    new Column("location", "location", STRUCT),
                    new Column("employer", "employer", ARRAY),
                    new Column("age", "age", INTEGER))),
            ImmutableList.of(
                tupleValue(
                    ImmutableMap.<String, Object>builder()
                        .put("name", "John")
                        .put("address1", "Seattle")
                        .put("address2", "WA")
                        .put("location", ImmutableMap.of("x", "1", "y", "2"))
                        .put(
                            "employments",
                            ImmutableList.of(
                                ImmutableMap.of("name", "Amazon"), ImmutableMap.of("name", "AWS")))
                        .put("age", 20)
                        .build())));

    assertJsonEquals(
        "{"
            + "\"schema\":["
            + "{\"name\":\"name\",\"alias\":\"name\",\"type\":\"keyword\"},"
            + "{\"name\":\"address1\",\"alias\":\"address1\",\"type\":\"text\"},"
            + "{\"name\":\"address2\",\"alias\":\"address2\",\"type\":\"text\"},"
            + "{\"name\":\"location\",\"alias\":\"location\",\"type\":\"object\"},"
            + "{\"name\":\"employer\",\"alias\":\"employer\",\"type\":\"nested\"},"
            + "{\"name\":\"age\",\"alias\":\"age\",\"type\":\"integer\"}"
            + "],"
            + "\"datarows\":["
            + "[\"John\",\"Seattle\",\"WA\",{\"x\":\"1\",\"y\":\"2\"},"
            + "[{\"name\":\"Amazon\"},"
            + "{\"name\":\"AWS\"}],"
            + "20]],"
            + "\"total\":1,"
            + "\"size\":1,"
            + "\"status\":200}",
        formatter.format(response));
  }

  @Test
  void format_response_with_cursor() {
    QueryResult response =
        new QueryResult(
            new Schema(
                ImmutableList.of(
                    new Column("name", "name", STRING),
                    new Column("address", "address", OpenSearchTextType.of()),
                    new Column("age", "age", INTEGER))),
            ImmutableList.of(
                tupleValue(
                    ImmutableMap.<String, Object>builder()
                        .put("name", "John")
                        .put("address", "Seattle")
                        .put("age", 20)
                        .build())),
            new Cursor("test_cursor"));

    assertJsonEquals(
        "{"
            + "\"schema\":["
            + "{\"name\":\"name\",\"alias\":\"name\",\"type\":\"keyword\"},"
            + "{\"name\":\"address\",\"alias\":\"address\",\"type\":\"text\"},"
            + "{\"name\":\"age\",\"alias\":\"age\",\"type\":\"integer\"}"
            + "],"
            + "\"datarows\":["
            + "[\"John\",\"Seattle\",20]],"
            + "\"total\":1,"
            + "\"size\":1,"
            + "\"cursor\":\"test_cursor\","
            + "\"status\":200}",
        formatter.format(response));
  }

  @Test
  void format_response_with_missing_and_null_value() {
    QueryResult response =
        new QueryResult(
            new Schema(
                ImmutableList.of(
                    new Column("name", null, STRING), new Column("age", null, INTEGER))),
            Arrays.asList(
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("name", stringValue("John"), "age", LITERAL_MISSING)),
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("name", stringValue("Allen"), "age", LITERAL_NULL)),
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));

    assertEquals(
        "{\"schema\":[{\"name\":\"name\",\"type\":\"keyword\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"John\",null],[\"Allen\",null],"
            + "[\"Smith\",30]],\"total\":3,\"size\":3,\"status\":200}",
        formatter.format(response));
  }

  @Test
  void format_client_error_response_due_to_syntax_exception() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"SyntaxCheckException\","
            + "\"reason\":\"Invalid Query\","
            + "\"details\":\"Invalid query syntax\""
            + "},"
            + "\"status\":400}",
        formatter.format(new SyntaxCheckException("Invalid query syntax")));
  }

  @Test
  void format_client_error_response_due_to_semantic_exception() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"SemanticCheckException\","
            + "\"reason\":\"Invalid Query\","
            + "\"details\":\"Invalid query semantics\""
            + "},"
            + "\"status\":400}",
        formatter.format(new SemanticCheckException("Invalid query semantics")));
  }

  @Test
  void format_server_error_response() {
    assertJsonEquals(
        "{\"error\":"
            + "{\""
            + "type\":\"IllegalStateException\","
            + "\"reason\":\"There was internal problem at backend\","
            + "\"details\":\"Execution error\""
            + "},"
            + "\"status\":503}",
        formatter.format(new IllegalStateException("Execution error")));
  }

  @Test
  void format_server_error_response_due_to_opensearch() {
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
        formatter.format(
            new OpenSearchException(
                "all shards failed", new IllegalStateException("Execution error"))));
  }

  private static void assertJsonEquals(String expected, String actual) {
    assertEquals(JsonParser.parseString(expected), JsonParser.parseString(actual));
  }
}

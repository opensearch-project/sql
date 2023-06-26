/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.CONTENT_TYPE;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.protocol.response.QueryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CommandResponseFormatterTest {

  @Test
  public void produces_always_same_output_for_any_query_response() {
    var formatter = new CommandResponseFormatter();
    assertEquals(formatter.format(mock(QueryResult.class)),
        formatter.format(mock(QueryResult.class)));

    QueryResult response = new QueryResult(
        new ExecutionEngine.Schema(ImmutableList.of(
            new ExecutionEngine.Schema.Column("name", "name", STRING),
            new ExecutionEngine.Schema.Column("address", "address", OpenSearchTextType.of()),
            new ExecutionEngine.Schema.Column("age", "age", INTEGER))),
        ImmutableList.of(
            tupleValue(ImmutableMap.<String, Object>builder()
                    .put("name", "John")
                    .put("address", "Seattle")
                    .put("age", 20)
                .build())),
        new Cursor("test_cursor"));

    assertEquals("{\n"
                + "  \"succeeded\": true\n"
                + "}",
        formatter.format(response));
  }

  @Test
  public void formats_error_as_default_formatter() {
    var exception = new Exception("pewpew", new RuntimeException("meow meow"));
    assertEquals(new JdbcResponseFormatter(PRETTY).format(exception),
        new CommandResponseFormatter().format(exception));
  }

  @Test
  void testContentType() {
    var formatter = new CommandResponseFormatter();
    assertEquals(formatter.contentType(), CONTENT_TYPE);
  }
}

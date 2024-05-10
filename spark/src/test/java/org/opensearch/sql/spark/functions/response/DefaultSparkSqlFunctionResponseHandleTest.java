/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

class DefaultSparkSqlFunctionResponseHandleTest {

  @Test
  public void testConstruct() throws Exception {
    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(readJson());

    assertTrue(handle.hasNext());
    ExprValue value = handle.next();
    Map<String, ExprValue> row = value.tupleValue();
    assertEquals(ExprBooleanValue.of(true), row.get("col1"));
    assertEquals(new ExprLongValue(2), row.get("col2"));
    assertEquals(new ExprIntegerValue(3), row.get("col3"));
    assertEquals(new ExprShortValue(4), row.get("col4"));
    assertEquals(new ExprByteValue(5), row.get("col5"));
    assertEquals(new ExprDoubleValue(6.1), row.get("col6"));
    assertEquals(new ExprFloatValue(7.1), row.get("col7"));
    assertEquals(new ExprStringValue("2024-01-02 03:04:05.1234"), row.get("col8"));
    assertEquals(new ExprDateValue("2024-01-03 04:05:06.1234"), row.get("col9"));
    assertEquals(new ExprStringValue("some string"), row.get("col10"));

    ExecutionEngine.Schema schema = handle.schema();
    List<Column> columns = schema.getColumns();
    assertEquals("col1", columns.get(0).getName());
  }

  private JSONObject readJson() throws Exception {
    final URL url =
        DefaultSparkSqlFunctionResponseHandle.class.getResource(
            "/spark_execution_result_test.json");
    return new JSONObject(Files.readString(Paths.get(url.toURI())));
  }
}

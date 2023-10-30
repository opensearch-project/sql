package org.opensearch.sql.spark.functions.response;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprTupleValue.fromExprValueMap;

import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;

class DefaultSparkSqlFunctionResponseHandleTest {

  @Test
  void testJsonObject() {
    JSONObject jsonObject =
        new JSONObject(
            "{"
                + "\"data\": {"
                + "  \"applicationId\": \"some_spark_application_id\","
                + "  \"schema\": ["
                + "    {\"column_name\": \"column1\", \"data_type\": \"string\"},"
                + "    {\"column_name\": \"column2\", \"data_type\": \"integer\"},"
                + "    {\"column_name\": \"column3\", \"data_type\": \"boolean\"}"
                + "  ],"
                + "  \"result\": ["
                + "    {\"column1\": \"value1\", \"column2\": 123, \"column3\": true},"
                + "    {\"column1\": \"value2\", \"column2\": 456, \"column3\": false}"
                + "  ]"
                + "}"
                + "}");
    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(jsonObject);
    assertEquals(
        new ExecutionEngine.Schema(
            List.of(
                new ExecutionEngine.Schema.Column("column1", "column1", ExprCoreType.STRING),
                new ExecutionEngine.Schema.Column("column2", "column2", ExprCoreType.INTEGER),
                new ExecutionEngine.Schema.Column("column3", "column3", ExprCoreType.BOOLEAN))),
        handle.schema());

    assertTrue(handle.hasNext());
    assertTrue(
        fromExprValueMap(
                Map.of(
                    "column1",
                    new ExprStringValue("value1"),
                    "column2",
                    new ExprIntegerValue(123),
                    "column3",
                    ExprBooleanValue.of(true)))
            .equal(handle.next()));
    assertTrue(
        fromExprValueMap(
                Map.of(
                    "column1",
                    new ExprStringValue("value2"),
                    "column2",
                    new ExprIntegerValue(456),
                    "column3",
                    ExprBooleanValue.of(false)))
            .equal(handle.next()));
    assertFalse(handle.hasNext());
  }

  @Test
  void testJsonArrayObject() {
    // region attributes
    String data =
        " { 'attributes': [\n"
            + "          {\n"
            + "            'key': 'telemetry.sdk.language',\n"
            + "            'value': {\n"
            + "              'stringValue': 'python'\n"
            + "            }\n"
            + "          },\n"
            + "          {\n"
            + "            'key': 'telemetry.sdk.name',\n"
            + "            'value': {\n"
            + "              'stringValue': 'opentelemetry'\n"
            + "            }\n"
            + "          },\n"
            + "          {\n"
            + "            'key': 'telemetry.sdk.version',\n"
            + "            'value': {\n"
            + "              'stringValue': '1.19.0'\n"
            + "            }\n"
            + "          },\n"
            + "          {\n"
            + "            'key': 'service.namespace',\n"
            + "            'value': {\n"
            + "              'stringValue': 'opentelemetry-demo'\n"
            + "            }\n"
            + "          },\n"
            + "          {\n"
            + "            'key': 'service.name',\n"
            + "            'value': {\n"
            + "              'stringValue': 'recommendationservice'\n"
            + "            }\n"
            + "          },\n"
            + "          {\n"
            + "            'key': 'telemetry.auto.version',\n"
            + "            'value': {\n"
            + "              'stringValue': '0.40b0'\n"
            + "            }\n"
            + "          }\n"
            + "        ]\n }";
    // endregion attributes
    // region schema
    String schema = "{'column_name':'attributes','data_type':'array'}";
    // endregion schema

    JSONObject jsonObject =
        new JSONObject(
            format(
                "{"
                    + "\"data\": "
                    + "     {\n"
                    + "          \"applicationId\": \"00fd777k3k3ls20p\",\n"
                    + "          \"schema\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "          \"result\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]\n"
                    + "   }"
                    + "}",
                schema, data));

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(jsonObject);
    assertEquals(
        new ExecutionEngine.Schema(
            List.of(
                new ExecutionEngine.Schema.Column("attributes", "attributes", ExprCoreType.ARRAY))),
        handle.schema());

    assertEquals(true, handle.hasNext());
    ExprTupleValue tupleValue = (ExprTupleValue) handle.next();
    assertEquals(1, tupleValue.tupleValue().size());
    assertEquals(true, tupleValue.tupleValue().get("attributes").isArray());
    assertEquals(6, tupleValue.tupleValue().get("attributes").arrayValue().size());
    assertEquals(false, handle.hasNext());
  }

  @Test
  void testJsonNestedObject() {
    // region resourceSpans
    String data =
        "{\n"
            + "  'resourceSpans': {\n"
            + "    'scopeSpans': {\n"
            + "      'spans': {\n"
            + "          'key': 'rpc.system',\n"
            + "          'value': {\n"
            + "            'stringValue': 'grpc'\n"
            + "          }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    // endregion resourceSpans
    // region schema
    String schema = "{'column_name':'resourceSpans','data_type':'struct'}";
    // endregion schema

    JSONObject jsonObject =
        new JSONObject(
            format(
                "{"
                    + "\"data\": "
                    + "     {\n"
                    + "          \"applicationId\": \"00fd777k3k3ls20p\",\n"
                    + "          \"schema\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "          \"result\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]\n"
                    + "   }"
                    + "}",
                schema, data));

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(jsonObject);
    assertEquals(
        new ExecutionEngine.Schema(
            List.of(
                new ExecutionEngine.Schema.Column(
                    "resourceSpans", "resourceSpans", ExprCoreType.STRUCT))),
        handle.schema());

    assertEquals(true, handle.hasNext());
    ExprTupleValue tupleValue = (ExprTupleValue) handle.next();
    assertEquals(1, tupleValue.tupleValue().size());
    assertEquals("resourceSpans", tupleValue.tupleValue().keySet().iterator().next());
    assertEquals(1, tupleValue.keyValue("resourceSpans").tupleValue().size());
    assertEquals(
        "scopeSpans", tupleValue.keyValue("resourceSpans").tupleValue().keySet().iterator().next());
    assertEquals(
        "spans",
        tupleValue
            .keyValue("resourceSpans")
            .keyValue("scopeSpans")
            .tupleValue()
            .keySet()
            .iterator()
            .next());
    assertEquals(
        2,
        tupleValue
            .tupleValue()
            .values()
            .iterator()
            .next()
            .keyValue("scopeSpans")
            .keyValue("spans")
            .tupleValue()
            .keySet()
            .size());

    assertEquals(false, handle.hasNext());
  }

  @Test
  void testJsonNestedObjectArray() {
    // region resourceSpans
    String data =
        "{\n"
            + "  'resourceSpans': \n"
            + "    {\n"
            + "      'scopeSpans': \n"
            + "        {\n"
            + "          'spans': \n"
            + "            [\n"
            + "              {\n"
            + "                'attribute': {\n"
            + "                  'key': 'rpc.system',\n"
            + "                  'value': {\n"
            + "                    'stringValue': 'grpc'\n"
            + "                  }\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                'attribute': {\n"
            + "                  'key': 'rpc.system',\n"
            + "                  'value': {\n"
            + "                    'stringValue': 'grpc'\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "        }\n"
            + "    }\n"
            + "}";
    // endregion resourceSpans
    // region schema
    String schema = "{'column_name':'resourceSpans','data_type':'struct'}";
    // endregion schema

    JSONObject jsonObject =
        new JSONObject(
            format(
                "{"
                    + "\"data\": "
                    + "     {\n"
                    + "          \"applicationId\": \"00fd777k3k3ls20p\",\n"
                    + "          \"schema\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "          \"result\": [\n"
                    + "            %s"
                    + "          ],\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]\n"
                    + "   }"
                    + "}",
                schema, data));

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(jsonObject);
    assertEquals(
        new ExecutionEngine.Schema(
            List.of(
                new ExecutionEngine.Schema.Column(
                    "resourceSpans", "resourceSpans", ExprCoreType.STRUCT))),
        handle.schema());

    assertEquals(true, handle.hasNext());
    ExprTupleValue tupleValue = (ExprTupleValue) handle.next();
    assertEquals(1, tupleValue.tupleValue().size());
    assertEquals("resourceSpans", tupleValue.tupleValue().keySet().iterator().next());
    assertEquals(1, tupleValue.keyValue("resourceSpans").tupleValue().size());
    assertEquals(
        "scopeSpans", tupleValue.keyValue("resourceSpans").tupleValue().keySet().iterator().next());
    assertEquals(
        "spans",
        tupleValue
            .keyValue("resourceSpans")
            .keyValue("scopeSpans")
            .tupleValue()
            .keySet()
            .iterator()
            .next());
    assertEquals(
        true,
        tupleValue
            .tupleValue()
            .values()
            .iterator()
            .next()
            .keyValue("scopeSpans")
            .keyValue("spans")
            .isArray());
    assertEquals(
        2,
        tupleValue
            .tupleValue()
            .values()
            .iterator()
            .next()
            .keyValue("scopeSpans")
            .keyValue("spans")
            .arrayValue()
            .size());
    assertEquals(
        "attribute",
        tupleValue
            .tupleValue()
            .values()
            .iterator()
            .next()
            .keyValue("scopeSpans")
            .keyValue("spans")
            .arrayValue()
            .get(0)
            .tupleValue()
            .keySet()
            .iterator()
            .next());
    assertEquals(
        "attribute",
        tupleValue
            .tupleValue()
            .values()
            .iterator()
            .next()
            .keyValue("scopeSpans")
            .keyValue("spans")
            .arrayValue()
            .get(1)
            .tupleValue()
            .keySet()
            .iterator()
            .next());

    assertEquals(false, handle.hasNext());
  }
}

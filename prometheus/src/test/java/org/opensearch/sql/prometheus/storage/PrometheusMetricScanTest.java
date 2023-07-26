/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.STEP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.prometheus.utils.TestUtils.getJson;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;

@ExtendWith(MockitoExtension.class)
public class PrometheusMetricScanTest {

  @Mock
  private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put(VALUE, new ExprDoubleValue(1));
        put("instance", new ExprStringValue("localhost:9090"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("prometheus"));
      }
    });
    assertEquals(firstRow, prometheusMetricScan.next());
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put("@timestamp", new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("@value", new ExprDoubleValue(0));
        put("instance", new ExprStringValue("localhost:9091"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("node"));
      }
    });
    assertEquals(secondRow, prometheusMetricScan.next());
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }

  @Test
  @SneakyThrows
  void testQueryResponseIteratorWithGivenPrometheusResponseFieldNames() {
    PrometheusResponseFieldNames prometheusResponseFieldNames
        = new PrometheusResponseFieldNames();
    prometheusResponseFieldNames.setValueFieldName("count()");
    prometheusResponseFieldNames.setValueType(INTEGER);
    prometheusResponseFieldNames.setTimestampFieldName(TIMESTAMP);
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.setPrometheusResponseFieldNames(prometheusResponseFieldNames);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("count()", new ExprIntegerValue(1));
        put("instance", new ExprStringValue("localhost:9090"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("prometheus"));
      }
    });
    assertEquals(firstRow, prometheusMetricScan.next());
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("count()", new ExprIntegerValue(0));
        put("instance", new ExprStringValue("localhost:9091"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("node"));
      }
    });
    assertEquals(secondRow, prometheusMetricScan.next());
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }


  @Test
  @SneakyThrows
  void testQueryResponseIteratorWithGivenPrometheusResponseWithLongInAggType() {
    PrometheusResponseFieldNames prometheusResponseFieldNames
        = new PrometheusResponseFieldNames();
    prometheusResponseFieldNames.setValueFieldName("testAgg");
    prometheusResponseFieldNames.setValueType(LONG);
    prometheusResponseFieldNames.setTimestampFieldName(TIMESTAMP);
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.setPrometheusResponseFieldNames(prometheusResponseFieldNames);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("testAgg", new ExprLongValue(1));
        put("instance", new ExprStringValue("localhost:9090"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("prometheus"));
      }
    });
    assertEquals(firstRow, prometheusMetricScan.next());
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("testAgg", new ExprLongValue(0));
        put("instance", new ExprStringValue("localhost:9091"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("node"));
      }
    });
    assertEquals(secondRow, prometheusMetricScan.next());
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }

  @Test
  @SneakyThrows
  void testQueryResponseIteratorWithGivenPrometheusResponseWithBackQuotedFieldNames() {
    PrometheusResponseFieldNames prometheusResponseFieldNames
        = new PrometheusResponseFieldNames();
    prometheusResponseFieldNames.setValueFieldName("testAgg");
    prometheusResponseFieldNames.setValueType(LONG);
    prometheusResponseFieldNames.setTimestampFieldName(TIMESTAMP);
    prometheusResponseFieldNames.setGroupByList(
        Collections.singletonList(DSL.named("`instance`", DSL.ref("instance", STRING))));
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.setPrometheusResponseFieldNames(prometheusResponseFieldNames);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("testAgg", new ExprLongValue(1));
        put("`instance`", new ExprStringValue("localhost:9090"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("prometheus"));
      }
    });
    assertEquals(firstRow, prometheusMetricScan.next());
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(TIMESTAMP, new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("testAgg", new ExprLongValue(0));
        put("`instance`", new ExprStringValue("localhost:9091"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("node"));
      }
    });
    assertEquals(secondRow, prometheusMetricScan.next());
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }


  @Test
  @SneakyThrows
  void testEmptyQueryResponseIterator() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("empty_query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithNoMatrixKeyInResultJson() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("no_matrix_query_range_result.json")));
    RuntimeException runtimeException
        = Assertions.assertThrows(RuntimeException.class, prometheusMetricScan::open);
    assertEquals(
        "Unexpected Result Type: vector during Prometheus Response Parsing. "
            + "'matrix' resultType is expected", runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, prometheusMetricScan::open);
    assertEquals("Error fetching data from prometheus server. Error Message",
        runtimeException.getMessage());
  }


  @Test
  @SneakyThrows
  void testExplain() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().setPromQl(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);
    assertEquals(
        "PrometheusQueryRequest(promQl=test_query, startTime=1664767694133, "
            + "endTime=1664771294133, step=14)",
        prometheusMetricScan.explain());
  }

}

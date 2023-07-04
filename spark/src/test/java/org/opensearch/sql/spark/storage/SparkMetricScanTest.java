/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.spark.client.SparkClient;

import java.time.Instant;
import java.util.LinkedHashMap;


@ExtendWith(MockitoExtension.class)
public class SparkMetricScanTest {
  @Mock
  private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testQueryResponseIteratorForQueryRangeFunction() {
    SparkMetricScan sparkMetricScan = new SparkMetricScan(sparkClient);
    sparkMetricScan.getRequest().setSql("select 1");
    Assertions.assertFalse(sparkMetricScan.hasNext());
    assertNull(sparkMetricScan.next());
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkMetricScan sparkMetricScan = new SparkMetricScan(sparkClient);
    sparkMetricScan.getRequest().setSql("select 1");
    assertEquals(
        "SparkQueryRequest(sql=select 1)",
        sparkMetricScan.explain());
  }
}
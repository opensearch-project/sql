/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.client.SparkClient;

@ExtendWith(MockitoExtension.class)
public class SparkMetricScanTest {
  @Mock
  private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testQueryResponseIteratorForQueryRangeFunction() {
    SparkMetricScan sparkMetricScan = new SparkMetricScan(sparkClient);
    sparkMetricScan.getRequest().setSql(QUERY);
    Assertions.assertFalse(sparkMetricScan.hasNext());
    assertNull(sparkMetricScan.next());
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkMetricScan sparkMetricScan = new SparkMetricScan(sparkClient);
    sparkMetricScan.getRequest().setSql(QUERY);
    assertEquals(
        "SparkQueryRequest(sql=select 1)",
        sparkMetricScan.explain());
  }
}

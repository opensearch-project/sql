/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.client.SparkClient;


@ExtendWith(MockitoExtension.class)
public class SparkMetricScanTest {
  @Mock
  private SparkClient client;

  @Test
  @SneakyThrows
  void testExplain() {
    SparkMetricScan sparkMetricScan = new SparkMetricScan(client);
    sparkMetricScan.getRequest().setSql("select 1");
    assertEquals(
        "SparkQueryRequest(sql=select 1)",
        sparkMetricScan.explain());
  }
}
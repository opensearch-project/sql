/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.spark.constants.TestConstants.SQL_QUERY;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.client.SparkClient;

@ExtendWith(MockitoExtension.class)
public class SparkScanTest {
  @Mock private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testQueryResponseIteratorForQueryRangeFunction() {
    SparkScan sparkScan = new SparkScan(sparkClient);
    sparkScan.getRequest().setSql(SQL_QUERY);
    Assertions.assertFalse(sparkScan.hasNext());
    assertNull(sparkScan.next());
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkScan sparkScan = new SparkScan(sparkClient);
    sparkScan.getRequest().setSql(SQL_QUERY);
    assertEquals("SparkQueryRequest(sql=select 1)", sparkScan.explain());
  }
}

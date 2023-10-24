/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

public class IndexDetailsTest {
  @Test
  public void skippingIndexName() {
    assertEquals(
        "flint_mys3_default_http_logs_skipping_index",
        IndexDetails.builder()
            .indexName("invalid")
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .autoRefresh(false)
            .isDropIndex(true)
            .indexType(FlintIndexType.SKIPPING)
            .build()
            .openSearchIndexName());
  }
}

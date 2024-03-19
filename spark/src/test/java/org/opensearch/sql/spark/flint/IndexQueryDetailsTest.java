/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

public class IndexQueryDetailsTest {
  @Test
  public void skippingIndexName() {
    assertEquals(
        "flint_mys3_default_http_logs_skipping_index",
        IndexQueryDetails.builder()
            .indexName("invalid")
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexOptions(new FlintIndexOptions())
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void coveringIndexName() {
    assertEquals(
        "flint_mys3_default_http_logs_idx_status_index",
        IndexQueryDetails.builder()
            .indexName("idx_status")
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexType(FlintIndexType.COVERING)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void materializedViewIndexName() {
    assertEquals(
        "flint_mys3_default_http_logs_metrics",
        IndexQueryDetails.builder()
            .mvName("mys3.default.http_logs_metrics")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void materializedViewIndexNameWithBackticks() {
    assertEquals(
        "flint_mys3_default_http_logs_metrics",
        IndexQueryDetails.builder()
            .mvName("`mys3`.`default`.`http_logs_metrics`")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void materializedViewIndexNameWithDots() {
    assertEquals(
        "flint_mys3_default_http_logs_metrics.1026",
        IndexQueryDetails.builder()
            .mvName("`mys3`.`default`.`http_logs_metrics.1026`")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void materializedViewIndexNameWithDotsInCatalogName() {
    // FIXME: should not use ctx.getText which is hard to split
    assertEquals(
        "flint_mys3_1026_default`.`http_logs_metrics",
        IndexQueryDetails.builder()
            .mvName("`mys3.1026`.`default`.`http_logs_metrics`")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());
  }

  @Test
  public void materializedViewIndexNameNotFullyQualified() {
    // Normally this should not happen and can add precondition check once confirmed.
    assertEquals(
        "flint_default_http_logs_metrics",
        IndexQueryDetails.builder()
            .mvName("default.http_logs_metrics")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());

    assertEquals(
        "flint_http_logs_metrics",
        IndexQueryDetails.builder()
            .mvName("http_logs_metrics")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build()
            .openSearchIndexName());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Spark table implementation.
 * This can be constructed from SparkQueryRequest.
 */
public class SparkMetricTable implements Table {

  private final SparkClient sparkClient;

  @Getter
  private final SparkQueryRequest sparkQueryRequest;

  /**
   * Constructor for entire Sql Request.
   */
  public SparkMetricTable(SparkClient sparkService,
                          @Nonnull SparkQueryRequest sparkQueryRequest) {
    this.sparkClient = sparkService;
    this.sparkQueryRequest = sparkQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "Exists operation is not supported in spark datasource");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "Create operation is not supported in spark datasource");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return new HashMap<>();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    //TODO: Add plan
    return null;
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    return new SqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
  }
}

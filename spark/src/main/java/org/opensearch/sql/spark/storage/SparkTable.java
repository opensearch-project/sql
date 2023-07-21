/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.storage;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Spark table implementation.
 * This can be constructed from SparkQueryRequest.
 */
public class SparkTable implements Table {

  private final SparkClient sparkClient;

  @Getter
  private final SparkQueryRequest sparkQueryRequest;

  /**
   * Constructor for entire Sql Request.
   */
  public SparkTable(SparkClient sparkService, SparkQueryRequest sparkQueryRequest) {
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
    SparkScan metricScan =
        new SparkScan(sparkClient);
    metricScan.setRequest(sparkQueryRequest);
    return plan.accept(new DefaultImplementor<SparkScan>(), metricScan);
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    return new SparkSqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
  }
}

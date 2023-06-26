/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.scan;

import lombok.AllArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * TableScanBuilder for sql function of spark connector.
 */
@AllArgsConstructor
public class SqlFunctionTableScanBuilder extends TableScanBuilder {

  private final SparkClient sparkClient;

  private final SparkQueryRequest sparkQueryRequest;

  @Override
  public TableScanOperator build() {
    //TODO: return SqlFunctionTableScanOperator
    return null;
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    return true;
  }
}
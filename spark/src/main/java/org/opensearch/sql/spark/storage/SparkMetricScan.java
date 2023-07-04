/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.response.SparkResponse;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Spark scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class SparkMetricScan extends TableScanOperator {

  private final SparkClient sparkClient;

  @EqualsAndHashCode.Include
  @Getter
  @Setter
  @ToString.Include
  private SparkQueryRequest request;


  /**
   * Constructor.
   *
   * @param sparkClient sparkClient.
   */
  public SparkMetricScan(SparkClient sparkClient) {
    this.sparkClient = sparkClient;
    this.request = new SparkQueryRequest();
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    return null;
  }

  @Override
  public String explain() {
    return getRequest().toString();
  }

}
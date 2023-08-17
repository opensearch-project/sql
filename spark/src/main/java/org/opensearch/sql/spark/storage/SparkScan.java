/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

/** Spark scan operator. */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class SparkScan extends TableScanOperator {

  private final SparkClient sparkClient;

  @EqualsAndHashCode.Include @Getter @Setter @ToString.Include private SparkQueryRequest request;

  /**
   * Constructor.
   *
   * @param sparkClient sparkClient.
   */
  public SparkScan(SparkClient sparkClient) {
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

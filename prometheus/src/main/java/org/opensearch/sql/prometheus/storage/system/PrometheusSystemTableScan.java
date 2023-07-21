/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.system;

import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.prometheus.request.system.PrometheusSystemRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Prometheus table scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusSystemTableScan extends TableScanOperator {

  @EqualsAndHashCode.Include
  private final PrometheusSystemRequest request;

  private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    iterator = request.search().iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public String explain() {
    return request.toString();
  }
}

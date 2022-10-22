/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.system;

import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.SessionContext;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * OpenSearch index scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class OpenSearchSystemIndexScan extends TableScanOperator {
  /**
   * OpenSearch client.
   */
  private final OpenSearchSystemRequest request;

  /**
   * Search response for current batch.
   */
  private Iterator<ExprValue> iterator;

  @Override
  public void open(SessionContext newContext) {
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

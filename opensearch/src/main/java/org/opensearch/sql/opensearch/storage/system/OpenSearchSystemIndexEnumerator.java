/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;

/** Supports a simple iteration over a collection for OpenSearch system index */
public class OpenSearchSystemIndexEnumerator implements Enumerator<Object> {
  /** How many moveNext() calls to perform resource check once. */
  private static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  private final List<String> fields;

  @EqualsAndHashCode.Include @ToString.Include private final OpenSearchSystemRequest request;

  private Iterator<ExprValue> iterator;

  private ExprValue current;

  /** Number of rows returned. */
  private Integer queryCount;

  /** ResourceMonitor. */
  private final ResourceMonitor monitor;

  public OpenSearchSystemIndexEnumerator(
      List<String> fields, OpenSearchSystemRequest request, ResourceMonitor monitor) {
    this.fields = fields;
    this.request = request;
    this.monitor = monitor;
    this.queryCount = 0;
    this.current = null;
    if (!this.monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to run the query, quit.");
    }
    this.iterator = request.search().iterator();
  }

  @Override
  public Object current() {
    return fields.stream()
        .map(k -> current.tupleValue().getOrDefault(k, ExprNullValue.of()).valueForCalcite())
        .toArray();
  }

  @Override
  public boolean moveNext() {
    boolean shouldCheck = (queryCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck && !this.monitor.isHealthy()) {
      throw new NonFallbackCalciteException("insufficient resources to load next row, quit.");
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      queryCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    iterator = request.search().iterator();
    queryCount = 0;
    current = null;
  }

  @Override
  public void close() {
    iterator = null;
    current = null;
  }
}

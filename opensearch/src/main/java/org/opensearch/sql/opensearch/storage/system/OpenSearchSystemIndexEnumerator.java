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
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;

/** Supports a simple iteration over a collection for OpenSearch system index */
public class OpenSearchSystemIndexEnumerator implements Enumerator<Object> {

  private final List<String> fields;

  @EqualsAndHashCode.Include @ToString.Include private final OpenSearchSystemRequest request;

  private Iterator<ExprValue> iterator;

  private ExprValue current;

  public OpenSearchSystemIndexEnumerator(List<String> fields, OpenSearchSystemRequest request) {
    this.fields = fields;
    this.request = request;
    this.current = null;
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
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    iterator = request.search().iterator();
    current = null;
  }

  @Override
  public void close() {
    iterator = null;
    current = null;
  }
}

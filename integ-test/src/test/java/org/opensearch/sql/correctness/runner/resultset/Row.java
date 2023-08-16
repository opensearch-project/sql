/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.runner.resultset;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Row in result set. */
@EqualsAndHashCode
@ToString
@Getter
public class Row implements Comparable<Row> {

  private final Collection<Object> values;

  public Row() {
    this(new ArrayList<>()); // values in order by default
  }

  public Row(Collection<Object> values) {
    this.values = values;
  }

  public void add(Object value) {
    values.add(roundFloatNum(value));
  }

  private Object roundFloatNum(Object value) {
    if (value instanceof Float) {
      BigDecimal decimal = BigDecimal.valueOf((Float) value).setScale(2, RoundingMode.CEILING);
      value = decimal.doubleValue(); // Convert to double too
    } else if (value instanceof Double) {
      BigDecimal decimal = BigDecimal.valueOf((Double) value).setScale(2, RoundingMode.CEILING);
      value = decimal.doubleValue();
    } else if (value instanceof BigDecimal) {
      value = ((BigDecimal) value).setScale(2, RoundingMode.CEILING).doubleValue();
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Row other) {
    List<Object> thisObjects = new ArrayList<>(values);
    List<Object> otherObjects = new ArrayList<>(other.values);

    for (int i = 0; i < thisObjects.size(); i++) {
      Object thisObject = thisObjects.get(i);
      Object otherObject = otherObjects.get(i);

      /*
       * Only one is null, otherwise (both null or non-null) go ahead.
       * Always consider NULL is greater which means NULL comes last in ASC and first in DESC
       */
      if (thisObject == null ^ otherObject == null) {
        return thisObject == null ? 1 : -1;
      }

      if (thisObject instanceof Comparable) {
        int result = ((Comparable) thisObject).compareTo(otherObject);
        if (result != 0) {
          return result;
        }
      } // Ignore incomparable field silently?
    }
    return 0;
  }
}

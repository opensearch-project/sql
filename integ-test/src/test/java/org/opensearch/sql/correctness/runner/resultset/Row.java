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
import java.util.Objects;
import lombok.Getter;
import lombok.ToString;

/** Row in result set. */
@ToString
@Getter
public class Row implements Comparable<Row> {

  private final Collection<Object> values;

  public Row() {
    this(new ArrayList<>());
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
      value = decimal.doubleValue();
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
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Row)) return false;
    Row other = (Row) o;
    return valuesEqual(this.values, other.values);
  }

  private boolean valuesEqual(Collection<Object> values1, Collection<Object> values2) {
    if (values1.size() != values2.size()) return false;

    List<Object> list1 = new ArrayList<>(values1);
    List<Object> list2 = new ArrayList<>(values2);

    for (int i = 0; i < list1.size(); i++) {
      if (!isValueEqual(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean isValueEqual(Object val1, Object val2) {
    if (Objects.equals(val1, val2)) return true;

    if (isIntegerOrLong(val1) && isIntegerOrLong(val2)) {
      return ((Number) val1).longValue() == ((Number) val2).longValue();
    }

    return false;
  }

  private boolean isIntegerOrLong(Object value) {
    return value instanceof Integer || value instanceof Long;
  }

  @Override
  public int hashCode() {

    List<Object> normalizedValues = new ArrayList<>();
    for (Object value : values) {
      normalizedValues.add(value instanceof Integer ? ((Integer) value).longValue() : value);
    }
    return normalizedValues.hashCode();
  }
}

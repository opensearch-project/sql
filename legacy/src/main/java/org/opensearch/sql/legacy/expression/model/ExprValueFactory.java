/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The definition of {@link ExprValue} factory. */
public class ExprValueFactory {

  public static ExprValue booleanValue(Boolean value) {
    return new ExprBooleanValue(value);
  }

  public static ExprValue integerValue(Integer value) {
    return new ExprIntegerValue(value);
  }

  public static ExprValue doubleValue(Double value) {
    return new ExprDoubleValue(value);
  }

  public static ExprValue stringValue(String value) {
    return new ExprStringValue(value);
  }

  public static ExprValue longValue(Long value) {
    return new ExprLongValue(value);
  }

  public static ExprValue tupleValue(Map<String, Object> map) {
    Map<String, ExprValue> valueMap = new HashMap<>();
    map.forEach((k, v) -> valueMap.put(k, from(v)));
    return new ExprTupleValue(valueMap);
  }

  public static ExprValue collectionValue(List<Object> list) {
    List<ExprValue> valueList = new ArrayList<>();
    list.forEach(o -> valueList.add(from(o)));
    return new ExprCollectionValue(valueList);
  }

  public static ExprValue from(Object o) {
    if (o instanceof Map) {
      return tupleValue((Map) o);
    } else if (o instanceof List) {
      return collectionValue(((List) o));
    } else if (o instanceof Integer) {
      return integerValue((Integer) o);
    } else if (o instanceof Long) {
      return longValue(((Long) o));
    } else if (o instanceof Boolean) {
      return booleanValue((Boolean) o);
    } else if (o instanceof Double) {
      return doubleValue((Double) o);
    } else if (o instanceof BigDecimal) {
      return doubleValue(((BigDecimal) o).doubleValue());
    } else if (o instanceof String) {
      return stringValue((String) o);
    } else {
      throw new IllegalStateException("unsupported type " + o.getClass());
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import inet.ipaddr.IPAddress;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

/** The definition of {@link ExprValue} factory. */
@UtilityClass
public class ExprValueUtils {
  public static final ExprValue LITERAL_TRUE = ExprBooleanValue.of(true);
  public static final ExprValue LITERAL_FALSE = ExprBooleanValue.of(false);
  public static final ExprValue LITERAL_NULL = ExprNullValue.of();
  public static final ExprValue LITERAL_MISSING = ExprMissingValue.of();

  public static ExprValue booleanValue(Boolean value) {
    return value ? LITERAL_TRUE : LITERAL_FALSE;
  }

  public static ExprValue byteValue(Byte value) {
    return new ExprByteValue(value);
  }

  public static ExprValue shortValue(Short value) {
    return new ExprShortValue(value);
  }

  public static ExprValue integerValue(Integer value) {
    return new ExprIntegerValue(value);
  }

  public static ExprValue doubleValue(Double value) {
    return new ExprDoubleValue(value);
  }

  public static ExprValue floatValue(Float value) {
    return new ExprFloatValue(value);
  }

  public static ExprValue longValue(Long value) {
    return new ExprLongValue(value);
  }

  public static ExprValue stringValue(String value) {
    return new ExprStringValue(value);
  }

  public static ExprValue intervalValue(TemporalAmount value) {
    return new ExprIntervalValue(value);
  }

  public static ExprValue dateValue(LocalDate value) {
    return new ExprDateValue(value);
  }

  public static ExprValue timeValue(LocalTime value) {
    return new ExprTimeValue(value);
  }

  public static ExprValue timestampValue(Instant value) {
    return new ExprTimestampValue(value);
  }

  public static ExprValue ipValue(String value) {
    return new ExprIpValue(value);
  }

  /** {@link ExprTupleValue} constructor. */
  public static ExprValue tupleValue(Map<String, Object> map) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    map.forEach(
        (k, v) -> valueMap.put(k, v instanceof ExprValue ? (ExprValue) v : fromObjectValue(v)));
    return new ExprTupleValue(valueMap);
  }

  /** {@link ExprCollectionValue} constructor. */
  public static ExprValue collectionValue(List<Object> list) {
    List<ExprValue> valueList = new ArrayList<>();
    list.forEach(o -> valueList.add(fromObjectValue(o)));
    return new ExprCollectionValue(valueList);
  }

  public static ExprValue missingValue() {
    return ExprMissingValue.of();
  }

  public static ExprValue nullValue() {
    return ExprNullValue.of();
  }

  /** Construct ExprValue from Object. */
  public static ExprValue fromObjectValue(Object o) {
    if (null == o) {
      return LITERAL_NULL;
    }
    if (o instanceof Map) {
      return tupleValue((Map) o);
    } else if (o instanceof List) {
      return collectionValue(((List) o));
    } else if (o instanceof Byte) {
      return byteValue((Byte) o);
    } else if (o instanceof Short) {
      return shortValue((Short) o);
    } else if (o instanceof Integer) {
      return integerValue((Integer) o);
    } else if (o instanceof Long) {
      return longValue(((Long) o));
    } else if (o instanceof Boolean) {
      return booleanValue((Boolean) o);
    } else if (o instanceof Double d) {
      if (Double.isNaN(d)) {
        return LITERAL_NULL;
      }
      return doubleValue(d);
    } else if (o instanceof String) {
      return stringValue((String) o);
    } else if (o instanceof Float f) {
      if (Float.isNaN(f)) {
        return LITERAL_NULL;
      }
      return floatValue(f);
    } else if (o instanceof Date) {
      return dateValue(((Date) o).toLocalDate());
    } else if (o instanceof LocalDate) {
      return dateValue((LocalDate) o);
    } else if (o instanceof Time) {
      return timeValue(((Time) o).toLocalTime());
    } else if (o instanceof LocalTime) {
      return timeValue((LocalTime) o);
    } else if (o instanceof Instant) {
      return timestampValue((Instant) o);
    } else if (o instanceof Timestamp) {
      return timestampValue(((Timestamp) o).toInstant());
    } else if (o instanceof LocalDateTime) {
      return timestampValue(((LocalDateTime) o).toInstant(ZoneOffset.UTC));
    } else if (o instanceof TemporalAmount) {
      return intervalValue((TemporalAmount) o);
    } else {
      throw new ExpressionEvaluationException("unsupported object " + o.getClass());
    }
  }

  /** Construct ExprValue from Object with ExprCoreType. */
  public static ExprValue fromObjectValue(Object o, ExprCoreType type) {
    switch (type) {
      case TIMESTAMP:
        return new ExprTimestampValue((String) o);
      case DATE:
        return new ExprDateValue((String) o);
      case TIME:
        return new ExprTimeValue((String) o);
      default:
        return fromObjectValue(o);
    }
  }

  public static Byte getByteValue(ExprValue exprValue) {
    return exprValue.byteValue();
  }

  public static Short getShortValue(ExprValue exprValue) {
    return exprValue.shortValue();
  }

  public static Integer getIntegerValue(ExprValue exprValue) {
    return exprValue.integerValue();
  }

  public static Double getDoubleValue(ExprValue exprValue) {
    return exprValue.doubleValue();
  }

  public static Long getLongValue(ExprValue exprValue) {
    return exprValue.longValue();
  }

  public static Float getFloatValue(ExprValue exprValue) {
    return exprValue.floatValue();
  }

  public static String getStringValue(ExprValue exprValue) {
    return exprValue.stringValue();
  }

  public static List<ExprValue> getCollectionValue(ExprValue exprValue) {
    return exprValue.collectionValue();
  }

  public static Map<String, ExprValue> getTupleValue(ExprValue exprValue) {
    return exprValue.tupleValue();
  }

  public static IPAddress getIpValue(ExprValue exprValue) {
    return exprValue.ipValue();
  }

  public static Boolean getBooleanValue(ExprValue exprValue) {
    return exprValue.booleanValue();
  }
}

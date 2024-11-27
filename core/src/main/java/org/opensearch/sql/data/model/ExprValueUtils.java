/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

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
    return switch (o) {
      case null -> LITERAL_NULL;
      case Map map -> tupleValue(map);
      case List list -> collectionValue(list);
      case Byte b -> byteValue(b);
      case Short i -> shortValue(i);
      case Integer i -> integerValue(i);
      case Long l -> longValue(l);
      case Boolean b -> booleanValue(b);
      case Double v -> doubleValue(v);
      case String s -> stringValue(s);
      case Float v -> floatValue(v);
      case LocalDate localDate -> dateValue(localDate);
      case LocalTime localTime -> timeValue(localTime);
      case Instant instant -> timestampValue(instant);
      case TemporalAmount temporalAmount -> intervalValue(temporalAmount);
      case LocalDateTime localDateTime -> timestampValue(localDateTime.toInstant(ZoneOffset.UTC));
      default -> throw new ExpressionEvaluationException("unsupported object " + o.getClass());
    };
  }

  /** Construct ExprValue from Object with ExprCoreType. */
  public static ExprValue fromObjectValue(Object o, ExprCoreType type) {
    return switch (type) {
      case TIMESTAMP -> new ExprTimestampValue((String) o);
      case DATE -> new ExprDateValue((String) o);
      case TIME -> new ExprTimeValue((String) o);
      default -> fromObjectValue(o);
    };
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

  public static Boolean getBooleanValue(ExprValue exprValue) {
    return exprValue.booleanValue();
  }
}

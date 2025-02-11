/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import inet.ipaddr.IPAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;

/** The definition of {@link ExprValue} factory. */
@UtilityClass
public class ExprValueUtils {

  // Literal constants
  public static final ExprValue LITERAL_TRUE = ExprBooleanValue.of(true);
  public static final ExprValue LITERAL_FALSE = ExprBooleanValue.of(false);
  public static final ExprValue LITERAL_NULL = ExprNullValue.of();
  public static final ExprValue LITERAL_MISSING = ExprMissingValue.of();

  /** Qualified name separator string */
  public final String QUALIFIED_NAME_SEPARATOR = ".";

  /** Pattern that matches the qualified name separator string */
  public final Pattern QUALIFIED_NAME_SEPARATOR_PATTERN =
      Pattern.compile(QUALIFIED_NAME_SEPARATOR, Pattern.LITERAL);

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
    } else if (o instanceof Double) {
      return doubleValue((Double) o);
    } else if (o instanceof String) {
      return stringValue((String) o);
    } else if (o instanceof Float) {
      return floatValue((Float) o);
    } else if (o instanceof LocalDate) {
      return dateValue((LocalDate) o);
    } else if (o instanceof LocalTime) {
      return timeValue((LocalTime) o);
    } else if (o instanceof Instant) {
      return timestampValue((Instant) o);
    } else if (o instanceof TemporalAmount) {
      return intervalValue((TemporalAmount) o);
    } else if (o instanceof LocalDateTime) {
      return timestampValue(((LocalDateTime) o).toInstant(ZoneOffset.UTC));
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

  /**
   * Splits the given qualified name into components and returns the result. Throws {@link
   * SemanticCheckException} if the qualified name is not valid.
   */
  public List<String> splitQualifiedName(String qualifiedName) {
    return Arrays.asList(QUALIFIED_NAME_SEPARATOR_PATTERN.split(qualifiedName));
  }

  /** Joins the given components into a qualified name and returns the result. */
  public String joinQualifiedName(List<String> components) {
    return String.join(QUALIFIED_NAME_SEPARATOR, components);
  }

  /**
   * Returns true if a nested {@link ExprValue} with the specified qualified name exists within the
   * given root value.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param qualifiedName qualified name for the nested {@link ExprValue} - e.g.
   *     'nested_struct.integer_field'
   */
  public boolean containsNestedExprValue(ExprValue rootExprValue, String qualifiedName) {
    List<String> components = splitQualifiedName(qualifiedName);
    return containsNestedExprValueForComponents(rootExprValue, components);
  }

  /**
   * Returns the nested {@link ExprValue} with the specified qualified name within the given root
   * value. Returns {@link ExprNullValue} if the root value does not contain a nested value with the
   * qualified name - see {@link ExprValueUtils#containsNestedExprValue}.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param qualifiedName qualified name for the nested {@link ExprValue} - e.g.
   *     'nested_struct.integer_field'
   */
  public ExprValue getNestedExprValue(ExprValue rootExprValue, String qualifiedName) {

    List<String> components = splitQualifiedName(qualifiedName);
    if (!containsNestedExprValueForComponents(rootExprValue, components)) {
      return nullValue();
    }

    return getNestedExprValueForComponents(rootExprValue, components);
  }

  /**
   * Sets the {@link ExprValue} with the specified qualified name within the given root value and
   * returns the result. Throws {@link SemanticCheckException} if the root value does not contain a
   * value with the qualified name - see {@link ExprValueUtils#containsNestedExprValue}.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param qualifiedName qualified name for the nested {@link ExprValue} - e.g.
   *     'nested_struct.integer_field'
   */
  public ExprValue setNestedExprValue(
      ExprValue rootExprValue, String qualifiedName, ExprValue newExprValue) {

    List<String> components = splitQualifiedName(qualifiedName);
    if (!containsNestedExprValueForComponents(rootExprValue, components)) {
      throw new SemanticCheckException(
          String.format("Field with qualified name '%s' does not exist.", qualifiedName));
    }

    return setNestedExprValueForComponents(rootExprValue, components, newExprValue);
  }

  /**
   * Returns true if a nested {@link ExprValue} exists within the given root value, at the location
   * specified by the qualified name components.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param components list of qualified name components - e.g. ['nested_struct','integer_field']
   */
  private boolean containsNestedExprValueForComponents(
      ExprValue rootExprValue, List<String> components) {

    if (components.isEmpty()) {
      return true;
    }

    if (!rootExprValue.type().equals(STRUCT)) {
      return false;
    }

    String currentComponent = components.getFirst();
    List<String> remainingComponents = components.subList(1, components.size());

    Map<String, ExprValue> exprValueMap = rootExprValue.tupleValue();
    if (!exprValueMap.containsKey(currentComponent)) {
      return false;
    }

    return containsNestedExprValueForComponents(
        exprValueMap.get(currentComponent), remainingComponents);
  }

  /**
   * Returns the nested {@link ExprValue} within the given root value, at the location specified by
   * the qualified name components. Requires that the root value contain a nested value with the
   * qualified name - see {@link ExprValueUtils#containsNestedExprValue}.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param components list of qualified name components - e.g. ['nested_struct','integer_field']
   */
  private ExprValue getNestedExprValueForComponents(
      ExprValue rootExprValue, List<String> components) {

    if (components.isEmpty()) {
      return rootExprValue;
    }

    String currentComponent = components.getFirst();
    List<String> remainingQualifiedNameComponents = components.subList(1, components.size());

    Map<String, ExprValue> exprValueMap = rootExprValue.tupleValue();
    return getNestedExprValueForComponents(
        exprValueMap.get(currentComponent), remainingQualifiedNameComponents);
  }

  /**
   * Sets the nested {@link ExprValue} within the given root value, at the location specified by the
   * qualified name components, and returns the result. Requires that the root value contain a
   * nested value with the qualified name - see {@link ExprValueUtils#containsNestedExprValue}.
   *
   * @param rootExprValue root value - expected to be an {@link ExprTupleValue}
   * @param components list of qualified name components - e.g. ['nested_struct','integer_field']
   */
  private ExprValue setNestedExprValueForComponents(
      ExprValue rootExprValue, List<String> components, ExprValue newExprValue) {

    if (components.isEmpty()) {
      return newExprValue;
    }

    String currentComponent = components.getFirst();
    List<String> remainingComponents = components.subList(1, components.size());

    Map<String, ExprValue> exprValueMap = new HashMap<>(rootExprValue.tupleValue());
    exprValueMap.put(
        currentComponent,
        setNestedExprValueForComponents(
            exprValueMap.get(currentComponent), remainingComponents, newExprValue));

    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}

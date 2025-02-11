/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;
import org.opensearch.sql.utils.IPUtils;

@DisplayName("Test Expression Value Utils")
public class ExprValueUtilsTest {

  // Test values
  private static final ExprValue byteExprValue = new ExprByteValue((byte) 1);
  private static final ExprValue shortExprValue = new ExprShortValue((short) 1);
  private static final ExprValue integerExprValue = new ExprIntegerValue(1);
  private static final ExprValue longExprValue = new ExprLongValue(1L);
  private static final ExprValue floatExprValue = new ExprFloatValue(1.0f);
  private static final ExprValue doubleExprValue = new ExprDoubleValue(1.0d);

  private static final ExprValue nullExprValue = ExprNullValue.of();
  private static final ExprValue missingExprValue = ExprMissingValue.of();

  private static final ExprValue ipExprValue = new ExprIpValue("1.2.3.4");
  private static final ExprValue stringExprValue = new ExprStringValue("value");
  private static final ExprValue booleanExprValue = ExprBooleanValue.of(true);
  private static final ExprValue dateExprValue = new ExprDateValue("2012-08-07");
  private static final ExprValue timeExprValue = new ExprTimeValue("18:00:00");
  private static final ExprValue timestampExprValue = new ExprTimestampValue("2012-08-07 18:00:00");
  private static final ExprValue intervalExprValue =
      new ExprIntervalValue(Duration.ofSeconds(100L));

  private static final ExprValue tupleExprValue =
      ExprTupleValue.fromExprValueMap(Map.of("integer", integerExprValue));
  private static final ExprValue collectionExprValue =
      new ExprCollectionValue(ImmutableList.of(integerExprValue));

  private final ExprValue tupleWithNestedExprValue =
      ExprTupleValue.fromExprValueMap(
          Map.of("tuple", ExprValueUtils.tupleValue(Map.of("integer", integerExprValue))));

  private final ExprValue rootExprValue =
      ExprTupleValue.fromExprValueMap(
          Map.ofEntries(
              Map.entry("integer", integerExprValue),
              Map.entry("tuple", tupleExprValue),
              Map.entry("tuple_with_nested", tupleWithNestedExprValue),
              Map.entry("null", ExprNullValue.of()),
              Map.entry("missing", ExprMissingValue.of())));

  private static final List<ExprValue> numberValues =
      List.of(
          byteExprValue,
          shortExprValue,
          integerExprValue,
          longExprValue,
          floatExprValue,
          doubleExprValue);

  private static final List<ExprValue> nonNumberValues =
      List.of(
          ipExprValue,
          stringExprValue,
          booleanExprValue,
          collectionExprValue,
          tupleExprValue,
          dateExprValue,
          timeExprValue,
          timestampExprValue,
          intervalExprValue);

  private static final List<ExprValue> allValues =
      Lists.newArrayList(Iterables.concat(numberValues, nonNumberValues));

  private static final List<Function<ExprValue, Object>> numberValueExtractor =
      List.of(
          ExprValueUtils::getByteValue,
          ExprValueUtils::getShortValue,
          ExprValueUtils::getIntegerValue,
          ExprValueUtils::getLongValue,
          ExprValueUtils::getFloatValue,
          ExprValueUtils::getDoubleValue);
  private static final List<Function<ExprValue, Object>> nonNumberValueExtractor =
      List.of(
          ExprValueUtils::getIpValue,
          ExprValueUtils::getStringValue,
          ExprValueUtils::getBooleanValue,
          ExprValueUtils::getCollectionValue,
          ExprValueUtils::getTupleValue);
  private static final List<Function<ExprValue, Object>> dateAndTimeValueExtractor =
      List.of(
          ExprValue::dateValue,
          ExprValue::timeValue,
          ExprValue::timestampValue,
          ExprValue::intervalValue);
  private static final List<Function<ExprValue, Object>> allValueExtractor =
      Lists.newArrayList(
          Iterables.concat(
              numberValueExtractor, nonNumberValueExtractor, dateAndTimeValueExtractor));

  private static final List<ExprCoreType> numberTypes =
      List.of(
          ExprCoreType.BYTE,
          ExprCoreType.SHORT,
          ExprCoreType.INTEGER,
          ExprCoreType.LONG,
          ExprCoreType.FLOAT,
          ExprCoreType.DOUBLE);
  private static final List<ExprCoreType> nonNumberTypes =
      List.of(IP, STRING, BOOLEAN, ARRAY, STRUCT);
  private static final List<ExprCoreType> dateAndTimeTypes =
      List.of(DATE, TIME, TIMESTAMP, INTERVAL);
  private static final List<ExprCoreType> allTypes =
      Lists.newArrayList(Iterables.concat(numberTypes, nonNumberTypes, dateAndTimeTypes));

  private static Stream<Arguments> getValueTestArgumentStream() {
    List<Object> expectedValues =
        List.of(
            (byte) 1,
            (short) 1,
            1,
            1L,
            1.0f,
            1.0d,
            IPUtils.toAddress("1.2.3.4"),
            "value",
            true,
            List.of(integerValue(1)),
            ImmutableMap.of("integer", integerValue(1)),
            LocalDate.parse("2012-08-07"),
            LocalTime.parse("18:00:00"),
            ZonedDateTime.of(LocalDateTime.parse("2012-08-07T18:00:00"), ZoneOffset.UTC)
                .toInstant(),
            Duration.ofSeconds(100L));
    Stream.Builder<Arguments> builder = Stream.builder();
    for (int i = 0; i < expectedValues.size(); i++) {
      builder.add(Arguments.of(allValues.get(i), allValueExtractor.get(i), expectedValues.get(i)));
    }
    return builder.build();
  }

  private static Stream<Arguments> getTypeTestArgumentStream() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (int i = 0; i < allValues.size(); i++) {
      builder.add(Arguments.of(allValues.get(i), allTypes.get(i)));
    }
    return builder.build();
  }

  private static Stream<Arguments> invalidGetNumberValueArgumentStream() {
    return Lists.cartesianProduct(nonNumberValues, numberValueExtractor).stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  @SuppressWarnings("unchecked")
  private static Stream<Arguments> invalidConvert() {
    List<Map.Entry<Function<ExprValue, Object>, ExprCoreType>> extractorWithTypeList =
        new ArrayList<>();
    for (int i = 0; i < nonNumberValueExtractor.size(); i++) {
      extractorWithTypeList.add(
          new AbstractMap.SimpleEntry<>(nonNumberValueExtractor.get(i), nonNumberTypes.get(i)));
    }
    return Lists.cartesianProduct(allValues, extractorWithTypeList).stream()
        .filter(
            list -> {
              ExprValue value = (ExprValue) list.get(0);
              Map.Entry<Function<ExprValue, Object>, ExprCoreType> entry =
                  (Map.Entry<Function<ExprValue, Object>, ExprCoreType>) list.get(1);
              return entry.getValue() != value.type();
            })
        .map(
            list -> {
              Map.Entry<Function<ExprValue, Object>, ExprCoreType> entry =
                  (Map.Entry<Function<ExprValue, Object>, ExprCoreType>) list.get(1);
              return Arguments.of(list.get(0), entry.getKey(), entry.getValue());
            });
  }

  @ParameterizedTest(name = "the value of ExprValue:{0} is: {2} ")
  @MethodSource("getValueTestArgumentStream")
  public void getValue(ExprValue value, Function<ExprValue, Object> extractor, Object expect) {
    assertEquals(expect, extractor.apply(value));
  }

  @ParameterizedTest(name = "the type of ExprValue:{0} is: {1} ")
  @MethodSource("getTypeTestArgumentStream")
  public void getType(ExprValue value, ExprCoreType expectType) {
    assertEquals(expectType, value.type());
  }

  /** Test Invalid to get number. */
  @ParameterizedTest(name = "invalid to get number value of ExprValue:{0}")
  @MethodSource("invalidGetNumberValueArgumentStream")
  public void invalidGetNumberValue(ExprValue value, Function<ExprValue, Object> extractor) {
    Exception exception =
        assertThrows(ExpressionEvaluationException.class, () -> extractor.apply(value));
    assertThat(exception.getMessage(), Matchers.containsString("invalid"));
  }

  /** Test Invalid to convert. */
  @ParameterizedTest(name = "invalid convert ExprValue:{0} to ExprType:{2}")
  @MethodSource("invalidConvert")
  public void invalidConvertExprValue(
      ExprValue value, Function<ExprValue, Object> extractor, ExprCoreType toType) {
    Exception exception =
        assertThrows(ExpressionEvaluationException.class, () -> extractor.apply(value));
    assertThat(exception.getMessage(), Matchers.containsString("invalid"));
  }

  @Test
  public void unSupportedObject() {
    Exception exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> ExprValueUtils.fromObjectValue(integerValue(1)));
    assertEquals(
        "unsupported object " + "class org.opensearch.sql.data.model.ExprIntegerValue",
        exception.getMessage());
  }

  @Test
  public void bindingTuples() {
    for (ExprValue value : allValues) {
      if (STRUCT == value.type()) {
        assertNotEquals(BindingTuple.EMPTY, value.bindingTuples());
      } else {
        assertEquals(BindingTuple.EMPTY, value.bindingTuples());
      }
    }
  }

  @Test
  public void constructDateAndTimeValue() {
    assertEquals(
        new ExprDateValue("2012-07-07"), ExprValueUtils.fromObjectValue("2012-07-07", DATE));
    assertEquals(new ExprTimeValue("01:01:01"), ExprValueUtils.fromObjectValue("01:01:01", TIME));
    assertEquals(
        new ExprTimestampValue("2012-07-07 01:01:01"),
        ExprValueUtils.fromObjectValue("2012-07-07 01:01:01", TIMESTAMP));
  }

  @Test
  public void hashCodeTest() {
    assertEquals(byteExprValue.hashCode(), new ExprByteValue((byte) 1).hashCode());
    assertEquals(shortExprValue.hashCode(), new ExprShortValue((short) 1).hashCode());
    assertEquals(integerExprValue.hashCode(), new ExprIntegerValue(1).hashCode());
    assertEquals(longExprValue.hashCode(), new ExprLongValue(1L).hashCode());
    assertEquals(floatExprValue.hashCode(), new ExprFloatValue(1.0f).hashCode());
    assertEquals(doubleExprValue.hashCode(), new ExprDoubleValue(1.0d).hashCode());

    assertEquals(nullExprValue.hashCode(), ExprNullValue.of().hashCode());
    assertEquals(missingExprValue.hashCode(), ExprMissingValue.of().hashCode());
    assertEquals(ipExprValue.hashCode(), new ExprIpValue("1.2.3.4").hashCode());
    assertEquals(stringExprValue.hashCode(), new ExprStringValue("value").hashCode());
    assertEquals(booleanExprValue.hashCode(), ExprBooleanValue.of(true).hashCode());
    assertEquals(dateExprValue.hashCode(), new ExprDateValue("2012-08-07").hashCode());
    assertEquals(timeExprValue.hashCode(), new ExprTimeValue("18:00:00").hashCode());
    assertEquals(
        timestampExprValue.hashCode(), new ExprTimestampValue("2012-08-07 18:00:00").hashCode());
    assertEquals(
        intervalExprValue.hashCode(), new ExprIntervalValue(Duration.ofSeconds(100L)).hashCode());
    assertEquals(
        tupleExprValue.hashCode(),
        ExprTupleValue.fromExprValueMap(Map.of("integer", integerExprValue)).hashCode());
    assertEquals(
        collectionExprValue.hashCode(),
        new ExprCollectionValue(ImmutableList.of(integerExprValue)).hashCode());
  }

  @Test
  void testContainsNestedExprValue() {
    assertTrue(ExprValueUtils.containsNestedExprValue(rootExprValue, "integer"));
    assertTrue(ExprValueUtils.containsNestedExprValue(rootExprValue, "tuple.integer"));
    assertTrue(
        ExprValueUtils.containsNestedExprValue(rootExprValue, "tuple_with_nested.tuple.integer"));

    assertFalse(ExprValueUtils.containsNestedExprValue(rootExprValue, "invalid"));
    assertFalse(ExprValueUtils.containsNestedExprValue(rootExprValue, "null.invalid"));
    assertFalse(ExprValueUtils.containsNestedExprValue(rootExprValue, "missing.invalid"));
    assertFalse(ExprValueUtils.containsNestedExprValue(rootExprValue, "invalid.invalid"));
  }

  @Test
  void testGetNestedExprValue() {
    assertEquals(integerExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "integer"));
    assertEquals(
        integerExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "tuple.integer"));
    assertEquals(
        integerExprValue,
        ExprValueUtils.getNestedExprValue(rootExprValue, "tuple_with_nested.tuple.integer"));

    assertEquals(nullExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "invalid"));
    assertEquals(nullExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "null.invalid"));
    assertEquals(
        nullExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "missing.invalid"));
    assertEquals(
        nullExprValue, ExprValueUtils.getNestedExprValue(rootExprValue, "invalid.invalid"));
  }

  @Test
  void testSetNestedExprValue() {
    ExprValue expected;
    ExprValue actual;

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("integer", stringExprValue),
                Map.entry("tuple", tupleExprValue),
                Map.entry("tuple_with_nested", tupleWithNestedExprValue),
                Map.entry("null", nullExprValue),
                Map.entry("missing", missingExprValue)));
    actual = ExprValueUtils.setNestedExprValue(rootExprValue, "integer", stringExprValue);
    assertEquals(expected, actual);

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("integer", integerExprValue),
                Map.entry("tuple", ExprValueUtils.tupleValue(Map.of("integer", stringExprValue))),
                Map.entry("tuple_with_nested", tupleWithNestedExprValue),
                Map.entry("null", nullExprValue),
                Map.entry("missing", missingExprValue)));
    actual = ExprValueUtils.setNestedExprValue(rootExprValue, "tuple.integer", stringExprValue);
    assertEquals(expected, actual);

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("integer", integerExprValue),
                Map.entry("tuple", tupleExprValue),
                Map.entry(
                    "tuple_with_nested",
                    ExprValueUtils.tupleValue(
                        Map.of(
                            "tuple",
                            ExprValueUtils.tupleValue(Map.of("integer", stringExprValue))))),
                Map.entry("null", nullExprValue),
                Map.entry("missing", missingExprValue)));
    assertEquals(
        expected,
        ExprValueUtils.setNestedExprValue(
            rootExprValue, "tuple_with_nested.tuple.integer", stringExprValue));

    Exception ex;

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> ExprValueUtils.setNestedExprValue(rootExprValue, "invalid", stringExprValue));
    assertEquals("Field with qualified name 'invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                ExprValueUtils.setNestedExprValue(rootExprValue, "null.invalid", stringExprValue));
    assertEquals("Field with qualified name 'null.invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                ExprValueUtils.setNestedExprValue(
                    rootExprValue, "missing.invalid", stringExprValue));
    assertEquals("Field with qualified name 'missing.invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                ExprValueUtils.setNestedExprValue(
                    rootExprValue, "invalid.invalid", stringExprValue));
    assertEquals("Field with qualified name 'invalid.invalid' does not exist.", ex.getMessage());
  }
}

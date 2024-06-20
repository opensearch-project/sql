/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.byteValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.shortValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;

class OpenSearchExprValueFactoryTest {

  private static final Map<String, OpenSearchDataType> MAPPING =
      new ImmutableMap.Builder<String, OpenSearchDataType>()
          .put("byteV", OpenSearchDataType.of(BYTE))
          .put("shortV", OpenSearchDataType.of(SHORT))
          .put("intV", OpenSearchDataType.of(INTEGER))
          .put("longV", OpenSearchDataType.of(LONG))
          .put("floatV", OpenSearchDataType.of(FLOAT))
          .put("doubleV", OpenSearchDataType.of(DOUBLE))
          .put("stringV", OpenSearchDataType.of(STRING))
          .put("dateV", OpenSearchDateType.of(DATE))
          .put("timeV", OpenSearchDateType.of(TIME))
          .put("timestampV", OpenSearchDateType.of(TIMESTAMP))
          .put("datetimeDefaultV", OpenSearchDateType.of())
          .put("dateStringV", OpenSearchDateType.of("date"))
          .put("timeStringV", OpenSearchDateType.of("time"))
          .put("epochMillisV", OpenSearchDateType.of("epoch_millis"))
          .put("epochSecondV", OpenSearchDateType.of("epoch_second"))
          .put("timeCustomV", OpenSearchDateType.of("HHmmss"))
          .put("dateCustomV", OpenSearchDateType.of("uuuuMMdd"))
          .put("dateTimeCustomV", OpenSearchDateType.of("uuuuMMddHHmmss"))
          .put("dateOrEpochMillisV", OpenSearchDateType.of("date_time_no_millis || epoch_millis"))
          .put("timeNoMillisOrTimeV", OpenSearchDateType.of("time_no_millis || time"))
          .put("dateOrOrdinalDateV", OpenSearchDateType.of("date || ordinal_date"))
          .put("customFormatV", OpenSearchDateType.of("yyyy-MM-dd-HH-mm-ss"))
          .put(
              "customAndEpochMillisV", OpenSearchDateType.of("yyyy-MM-dd-HH-mm-ss || epoch_millis"))
          .put("incompleteFormatV", OpenSearchDateType.of("year"))
          .put("boolV", OpenSearchDataType.of(BOOLEAN))
          .put("structV", OpenSearchDataType.of(STRUCT))
          .put("structV.id", OpenSearchDataType.of(INTEGER))
          .put("structV.state", OpenSearchDataType.of(STRING))
          .put("arrayV", OpenSearchDataType.of(ARRAY))
          .put("arrayV.info", OpenSearchDataType.of(STRING))
          .put("arrayV.author", OpenSearchDataType.of(STRING))
          .put(
              "deepNestedV",
              OpenSearchDataType.of(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested)))
          .put(
              "deepNestedV.year",
              OpenSearchDataType.of(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested)))
          .put("deepNestedV.year.timeV", OpenSearchDateType.of(TIME))
          .put(
              "nestedV",
              OpenSearchDataType.of(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested)))
          .put("nestedV.count", OpenSearchDataType.of(INTEGER))
          .put("textV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Text))
          .put(
              "textKeywordV",
              OpenSearchTextType.of(
                  Map.of("words", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))))
          .put("ipV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Ip))
          .put("geoV", OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint))
          .put("binaryV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Binary))
          .build();

  private final OpenSearchExprValueFactory exprValueFactory =
      new OpenSearchExprValueFactory(MAPPING);

  @Test
  public void constructNullValue() {
    assertAll(
        () -> assertEquals(nullValue(), tupleValue("{\"intV\":null}").get("intV")),
        () -> assertEquals(nullValue(), constructFromObject("intV", null)),
        () -> assertTrue(new OpenSearchJsonContent(null).isNull()));
  }

  @Test
  public void iterateArrayValue() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    var arrayIt = new OpenSearchJsonContent(mapper.readTree("[\"zz\",\"bb\"]")).array();
    assertAll(
        () -> assertEquals("zz", arrayIt.next().stringValue()),
        () -> assertEquals("bb", arrayIt.next().stringValue()),
        () -> assertFalse(arrayIt.hasNext()));
  }

  @Test
  public void iterateArrayValueWithOneElement() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    var arrayIt = new OpenSearchJsonContent(mapper.readTree("[\"zz\"]")).array();
    assertAll(
        () -> assertEquals("zz", arrayIt.next().stringValue()),
        () -> assertFalse(arrayIt.hasNext()));
  }

  @Test
  public void constructNullArrayValue() {
    assertEquals(nullValue(), tupleValue("{\"intV\":[]}").get("intV"));
  }

  @Test
  public void constructByte() {
    assertAll(
        () -> assertEquals(byteValue((byte) 1), tupleValue("{\"byteV\":1}").get("byteV")),
        () -> assertEquals(byteValue((byte) 1), constructFromObject("byteV", 1)),
        () -> assertEquals(byteValue((byte) 1), constructFromObject("byteV", "1.0")));
  }

  @Test
  public void constructShort() {
    assertAll(
        () -> assertEquals(shortValue((short) 1), tupleValue("{\"shortV\":1}").get("shortV")),
        () -> assertEquals(shortValue((short) 1), constructFromObject("shortV", 1)),
        () -> assertEquals(shortValue((short) 1), constructFromObject("shortV", "1.0")));
  }

  @Test
  public void constructInteger() {
    assertAll(
        () -> assertEquals(integerValue(1), tupleValue("{\"intV\":1}").get("intV")),
        () -> assertEquals(integerValue(1), constructFromObject("intV", 1)),
        () -> assertEquals(integerValue(1), constructFromObject("intV", "1.0")));
  }

  @Test
  public void constructIntegerValueInStringValue() {
    assertEquals(integerValue(1), constructFromObject("intV", "1"));
  }

  @Test
  public void constructLong() {
    assertAll(
        () -> assertEquals(longValue(1L), tupleValue("{\"longV\":1}").get("longV")),
        () -> assertEquals(longValue(1L), constructFromObject("longV", 1L)),
        () -> assertEquals(longValue(1L), constructFromObject("longV", "1.0")));
  }

  @Test
  public void constructFloat() {
    assertAll(
        () -> assertEquals(floatValue(1f), tupleValue("{\"floatV\":1.0}").get("floatV")),
        () -> assertEquals(floatValue(1f), constructFromObject("floatV", 1f)));
  }

  @Test
  public void constructDouble() {
    assertAll(
        () -> assertEquals(doubleValue(1d), tupleValue("{\"doubleV\":1.0}").get("doubleV")),
        () -> assertEquals(doubleValue(1d), constructFromObject("doubleV", 1d)));
  }

  @Test
  public void constructString() {
    assertAll(
        () ->
            assertEquals(stringValue("text"), tupleValue("{\"stringV\":\"text\"}").get("stringV")),
        () -> assertEquals(stringValue("text"), constructFromObject("stringV", "text")));
  }

  @Test
  public void constructBoolean() {
    assertAll(
        () -> assertEquals(booleanValue(true), tupleValue("{\"boolV\":true}").get("boolV")),
        () -> assertEquals(booleanValue(true), constructFromObject("boolV", true)),
        () -> assertEquals(booleanValue(true), constructFromObject("boolV", "true")),
        () -> assertEquals(booleanValue(true), constructFromObject("boolV", 1)),
        () -> assertEquals(booleanValue(false), constructFromObject("boolV", 0)));
  }

  @Test
  public void constructText() {
    assertAll(
        () ->
            assertEquals(
                new OpenSearchExprTextValue("text"),
                tupleValue("{\"textV\":\"text\"}").get("textV")),
        () ->
            assertEquals(new OpenSearchExprTextValue("text"), constructFromObject("textV", "text")),
        () ->
            assertEquals(
                new OpenSearchExprTextValue("text"),
                tupleValue("{\"textKeywordV\":\"text\"}").get("textKeywordV")),
        () ->
            assertEquals(
                new OpenSearchExprTextValue("text"), constructFromObject("textKeywordV", "text")));
  }

  @Test
  public void constructDates() {
    ExprValue dateStringV = constructFromObject("dateStringV", "1984-04-12");
    assertAll(
        () -> assertEquals(new ExprDateValue("1984-04-12"), dateStringV),
        () ->
            assertEquals(
                new ExprDateValue(
                    LocalDate.ofInstant(Instant.ofEpochMilli(450576000000L), ZoneOffset.UTC)),
                constructFromObject("dateV", 450576000000L)),
        () ->
            assertEquals(
                new ExprDateValue("1984-04-12"),
                constructFromObject("dateOrOrdinalDateV", "1984-103")),
        () ->
            assertEquals(
                new ExprDateValue("2015-01-01"),
                tupleValue("{\"dateV\":\"2015-01-01\"}").get("dateV")));
  }

  @Test
  public void constructTimes() {
    ExprValue timeStringV = constructFromObject("timeStringV", "12:10:30.000Z");
    assertAll(
        () -> assertTrue(timeStringV.isDateTime()),
        () -> assertTrue(timeStringV instanceof ExprTimeValue),
        () -> assertEquals(new ExprTimeValue("12:10:30"), timeStringV),
        () ->
            assertEquals(
                new ExprTimeValue(
                    LocalTime.from(Instant.ofEpochMilli(1420070400001L).atZone(ZoneOffset.UTC))),
                constructFromObject("timeV", 1420070400001L)),
        () ->
            assertEquals(
                new ExprTimeValue("09:07:42.000"),
                constructFromObject("timeNoMillisOrTimeV", "09:07:42.000Z")),
        () ->
            assertEquals(
                new ExprTimeValue("09:07:42"),
                tupleValue("{\"timeV\":\"09:07:42\"}").get("timeV")));
  }

  @Test
  public void constructDatetime() {
    assertAll(
        () ->
            assertEquals(
                new ExprTimestampValue("2015-01-01 00:00:00"),
                tupleValue("{\"timestampV\":\"2015-01-01\"}").get("timestampV")),
        () ->
            assertEquals(
                new ExprTimestampValue("2015-01-01 12:10:30"),
                tupleValue("{\"timestampV\":\"2015-01-01T12:10:30Z\"}").get("timestampV")),
        () ->
            assertEquals(
                new ExprTimestampValue("2015-01-01 12:10:30"),
                tupleValue("{\"timestampV\":\"2015-01-01T12:10:30\"}").get("timestampV")),
        () ->
            assertEquals(
                new ExprTimestampValue("2015-01-01 12:10:30"),
                tupleValue("{\"timestampV\":\"2015-01-01 12:10:30\"}").get("timestampV")),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
                constructFromObject("timestampV", 1420070400001L)),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
                constructFromObject("timestampV", Instant.ofEpochMilli(1420070400001L))),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
                constructFromObject("epochMillisV", "1420070400001")),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
                constructFromObject("epochMillisV", 1420070400001L)),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochSecond(142704001L)),
                constructFromObject("epochSecondV", 142704001L)),
        () ->
            assertEquals(
                new ExprTimeValue("10:20:30"),
                tupleValue("{ \"timeCustomV\" : 102030 }").get("timeCustomV")),
        () ->
            assertEquals(
                new ExprDateValue("1961-04-12"),
                tupleValue("{ \"dateCustomV\" : 19610412 }").get("dateCustomV")),
        () ->
            assertEquals(
                new ExprTimestampValue("1984-05-10 20:30:40"),
                tupleValue("{ \"dateTimeCustomV\" : 19840510203040 }").get("dateTimeCustomV")),
        () ->
            assertEquals(
                new ExprTimestampValue("2015-01-01 12:10:30"),
                constructFromObject("timestampV", "2015-01-01 12:10:30")),
        () ->
            assertEquals(
                new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
                constructFromObject("dateOrEpochMillisV", "1420070400001")),

        // case: timestamp-formatted field, but it only gets a time: should match a time
        () ->
            assertEquals(
                new ExprTimeValue("19:36:22"),
                tupleValue("{\"timestampV\":\"19:36:22\"}").get("timestampV")),

        // case: timestamp-formatted field, but it only gets a date: should match a date
        () ->
            assertEquals(
                new ExprDateValue("2011-03-03"),
                tupleValue("{\"timestampV\":\"2011-03-03\"}").get("timestampV")));
  }

  @Test
  public void constructDatetime_fromCustomFormat() {
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        constructFromObject("customFormatV", "2015-01-01-12-10-30"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> constructFromObject("customFormatV", "2015-01-01 12-10-30"));
    assertEquals(
        "Construct TIMESTAMP from \"2015-01-01 12-10-30\" failed, unsupported format.",
        exception.getMessage());

    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        constructFromObject("customAndEpochMillisV", "2015-01-01 12:10:30"));

    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        constructFromObject("customAndEpochMillisV", "2015-01-01-12-10-30"));
  }

  @Test
  public void constructDatetimeFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> constructFromObject("timestampV", "2015-01-01 12:10"));
    assertEquals(
        "Construct TIMESTAMP from \"2015-01-01 12:10\" failed, unsupported format.",
        exception.getMessage());

    // fail with missing seconds
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> constructFromObject("dateOrEpochMillisV", "2015-01-01 12:10"));
    assertEquals(
        "Construct TIMESTAMP from \"2015-01-01 12:10\" failed, unsupported format.",
        exception.getMessage());
  }

  @Test
  public void constructTimeFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> constructFromObject("timeV", "2015-01-01"));
    assertEquals(
        "Construct TIME from \"2015-01-01\" failed, unsupported format.", exception.getMessage());

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> constructFromObject("timeStringV", "10:10"));
    assertEquals(
        "Construct TIME from \"10:10\" failed, unsupported format.", exception.getMessage());
  }

  @Test
  public void constructDateFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> constructFromObject("dateV", "12:10:10"));
    assertEquals(
        "Construct DATE from \"12:10:10\" failed, unsupported format.", exception.getMessage());

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> constructFromObject("dateStringV", "abc"));
    assertEquals("Construct DATE from \"abc\" failed, unsupported format.", exception.getMessage());
  }

  @Test
  public void constructDateFromIncompleteFormat() {
    assertEquals(new ExprDateValue("1984-01-01"), constructFromObject("incompleteFormatV", "1984"));
  }

  @Test
  public void constructArray() {
    assertEquals(
        new ExprCollectionValue(
            List.of(
                new ExprTupleValue(
                    new LinkedHashMap<String, ExprValue>() {
                      {
                        put("info", stringValue("zz"));
                        put("author", stringValue("au"));
                      }
                    }))),
        tupleValue("{\"arrayV\":[{\"info\":\"zz\",\"author\":\"au\"}]}").get("arrayV"));
    assertEquals(
        new ExprCollectionValue(
            List.of(
                new ExprTupleValue(
                    new LinkedHashMap<String, ExprValue>() {
                      {
                        put("info", stringValue("zz"));
                        put("author", stringValue("au"));
                      }
                    }))),
        constructFromObject("arrayV", List.of(ImmutableMap.of("info", "zz", "author", "au"))));
  }

  @Test
  public void constructArrayOfStrings() {
    assertEquals(
        new ExprCollectionValue(List.of(stringValue("zz"), stringValue("au"))),
        constructFromObject("arrayV", List.of("zz", "au")));
  }

  @Test
  public void constructNestedArraysOfStrings() {
    assertEquals(
        new ExprCollectionValue(
            List.of(collectionValue(List.of("zz", "au")), collectionValue(List.of("ss")))),
        tupleValueWithArraySupport("{\"stringV\":[ [\"zz\", \"au\"], [\"ss\"] ]}").get("stringV"));
  }

  @Test
  public void constructNestedArraysOfStringsReturnsFirstIndex() {
    assertEquals(
        stringValue("zz"), tupleValue("{\"stringV\":[[\"zz\", \"au\"],[\"ss\"]]}").get("stringV"));
  }

  @Test
  public void constructMultiNestedArraysOfStringsReturnsFirstIndex() {
    assertEquals(
        stringValue("z"),
        tupleValue("{\"stringV\":[\"z\",[\"s\"],[\"zz\", \"au\"]]}").get("stringV"));
  }

  @Test
  public void constructArrayOfInts() {
    assertEquals(
        new ExprCollectionValue(List.of(integerValue(1), integerValue(2))),
        constructFromObject("arrayV", List.of(1, 2)));
  }

  @Test
  public void constructArrayOfShorts() {
    // Shorts are treated same as integer
    assertEquals(
        new ExprCollectionValue(List.of(shortValue((short) 3), shortValue((short) 4))),
        constructFromObject("arrayV", List.of(3, 4)));
  }

  @Test
  public void constructArrayOfLongs() {
    assertEquals(
        new ExprCollectionValue(List.of(longValue(123456789L), longValue(987654321L))),
        constructFromObject("arrayV", List.of(123456789L, 987654321L)));
  }

  @Test
  public void constructArrayOfFloats() {
    assertEquals(
        new ExprCollectionValue(List.of(floatValue(3.14f), floatValue(4.13f))),
        constructFromObject("arrayV", List.of(3.14f, 4.13f)));
  }

  @Test
  public void constructArrayOfDoubles() {
    assertEquals(
        new ExprCollectionValue(List.of(doubleValue(9.1928374756D), doubleValue(4.987654321D))),
        constructFromObject("arrayV", List.of(9.1928374756D, 4.987654321D)));
  }

  @Test
  public void constructArrayOfBooleans() {
    assertEquals(
        new ExprCollectionValue(List.of(booleanValue(true), booleanValue(false))),
        constructFromObject("arrayV", List.of(true, false)));
  }

  @Test
  public void constructNestedObjectArrayNode() {
    assertEquals(
        collectionValue(List.of(Map.of("count", 1), Map.of("count", 2))),
        tupleValueWithArraySupport("{\"nestedV\":[{\"count\":1},{\"count\":2}]}").get("nestedV"));
  }

  @Test
  public void constructNestedObjectArrayOfObjectArraysNode() {
    assertEquals(
        collectionValue(
            List.of(
                Map.of(
                    "year",
                    List.of(
                        Map.of("timeV", new ExprTimeValue("09:07:42")),
                        Map.of("timeV", new ExprTimeValue("09:07:42")))),
                Map.of(
                    "year",
                    List.of(
                        Map.of("timeV", new ExprTimeValue("09:07:42")),
                        Map.of("timeV", new ExprTimeValue("09:07:42")))))),
        tupleValueWithArraySupport(
                "{\"deepNestedV\":"
                    + "  ["
                    + "    {\"year\":"
                    + "      ["
                    + "        {\"timeV\":\"09:07:42\"},"
                    + "        {\"timeV\":\"09:07:42\"}"
                    + "      ]"
                    + "    },"
                    + "    {\"year\":"
                    + "      ["
                    + "        {\"timeV\":\"09:07:42\"},"
                    + "        {\"timeV\":\"09:07:42\"}"
                    + "      ]"
                    + "    }"
                    + "  ]"
                    + "}")
            .get("deepNestedV"));
  }

  @Test
  public void constructNestedArrayNode() {
    assertEquals(
        collectionValue(List.of(1969, 2011)),
        tupleValueWithArraySupport("{\"nestedV\":[1969,2011]}").get("nestedV"));
  }

  @Test
  public void constructNestedObjectNode() {
    assertEquals(
        collectionValue(List.of(Map.of("count", 1969))),
        tupleValue("{\"nestedV\":{\"count\":1969}}").get("nestedV"));
  }

  @Test
  public void constructArrayOfGeoPoints() {
    assertEquals(
        new ExprCollectionValue(
            List.of(
                new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
                new OpenSearchExprGeoPointValue(-33.6123556, 66.287449))),
        tupleValueWithArraySupport(
                "{\"geoV\":["
                    + "{\"lat\":42.60355556,\"lon\":-97.25263889},"
                    + "{\"lat\":-33.6123556,\"lon\":66.287449}"
                    + "]}")
            .get("geoV"));
  }

  @Test
  public void constructArrayOfIPsReturnsFirstIndex() {
    assertEquals(
        new OpenSearchExprIpValue("192.168.0.1"),
        tupleValue("{\"ipV\":[\"192.168.0.1\",\"192.168.0.2\"]}").get("ipV"));
  }

  @Test
  public void constructBinaryArrayReturnsFirstIndex() {
    assertEquals(
        new OpenSearchExprBinaryValue("U29tZSBiaWsdfsdfgYmxvYg=="),
        tupleValue("{\"binaryV\":[\"U29tZSBiaWsdfsdfgYmxvYg==\",\"U987yuhjjiy8jhk9vY+98jjdf\"]}")
            .get("binaryV"));
  }

  @Test
  public void constructArrayOfCustomEpochMillisReturnsFirstIndex() {
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"customAndEpochMillisV\":[\"2015-01-01 12:10:30\",\"1999-11-09 01:09:44\"]}")
            .get("customAndEpochMillisV"));
  }

  @Test
  public void constructArrayOfDateStringsReturnsFirstIndex() {
    assertEquals(
        new ExprDateValue("1984-04-12"),
        tupleValue("{\"dateStringV\":[\"1984-04-12\",\"2033-05-03\"]}").get("dateStringV"));
  }

  @Test
  public void constructArrayOfTimeStringsReturnsFirstIndex() {
    assertEquals(
        new ExprTimeValue("12:10:30"),
        tupleValue("{\"timeStringV\":[\"12:10:30.000Z\",\"18:33:55.000Z\"]}").get("timeStringV"));
  }

  @Test
  public void constructArrayOfEpochMillis() {
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        tupleValue("{\"dateOrEpochMillisV\":[\"1420070400001\",\"1454251113333\"]}")
            .get("dateOrEpochMillisV"));
  }

  @Test
  public void constructStruct() {
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("id", integerValue(1));
                put("state", stringValue("WA"));
              }
            }),
        tupleValue("{\"structV\":{\"id\":1,\"state\":\"WA\"}}").get("structV"));
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("id", integerValue(1));
                put("state", stringValue("WA"));
              }
            }),
        constructFromObject("structV", ImmutableMap.of("id", 1, "state", "WA")));
  }

  @Test
  public void constructIP() {
    assertEquals(
        new OpenSearchExprIpValue("192.168.0.1"),
        tupleValue("{\"ipV\":\"192.168.0.1\"}").get("ipV"));
  }

  @Test
  public void constructGeoPoint() {
    assertEquals(
        new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals(
        new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":\"42.60355556\",\"lon\":\"-97.25263889\"}}").get("geoV"));
    assertEquals(
        new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        constructFromObject("geoV", "42.60355556,-97.25263889"));
  }

  @Test
  public void constructGeoPointLatLon() {
    assertEquals(
        doubleValue(42.60355556),
        tupleValue("{\"geoV\":{\"lat\":42.60355556}}").get("geoV").tupleValue().get("lat"));
    assertEquals(
        doubleValue(-97.25263889),
        tupleValue("{\"geoV\":{\"lon\":-97.25263889}}").get("geoV").tupleValue().get("lon"));
  }

  @Test
  public void constructGeoPointFromUnsupportedFormatShouldThrowException() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> tupleValue("{\"geoV\":[42.60355556,-97.25263889]}").get("geoV"));
    assertEquals(
        "geo point must be in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> tupleValue("{\"geoV\":\"txhxegj0uyp3\"}").get("geoV"));
    assertEquals(
        "geo point must be in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                tupleValue("{\"geoV\":{\"type\": \"Point\"," + " \"coordinates\": [74.00, 40.71]}}")
                    .get("geoV"));
    assertEquals(
        "geo point must be in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":true,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals("latitude must be number value, but got value: true", exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":false}}").get("geoV"));
    assertEquals("longitude must be number value, but got value: false", exception.getMessage());
  }

  @Test
  public void constructBinary() {
    assertEquals(
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg=="),
        tupleValue("{\"binaryV\":\"U29tZSBiaW5hcnkgYmxvYg==\"}").get("binaryV"));
  }

  /**
   * Return the first element if is OpenSearch Array.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html.
   */
  @Test
  public void constructFromOpenSearchArrayReturnFirstElement() {
    assertEquals(integerValue(1), tupleValue("{\"intV\":[1, 2, 3]}").get("intV"));
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("id", integerValue(1));
                put("state", stringValue("WA"));
              }
            }),
        tupleValue("{\"structV\":[{\"id\":1,\"state\":\"WA\"},{\"id\":2,\"state\":\"CA\"}]}}")
            .get("structV"));
  }

  @Test
  public void constructFromInvalidJsonThrowException() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> tupleValue("{\"invalid_json:1}"));
    assertEquals("invalid json: {\"invalid_json:1}.", exception.getMessage());
  }

  @Test
  public void noTypeFoundForMapping() {
    assertEquals(nullValue(), tupleValue("{\"not_exist\":[]}").get("not_exist"));
    // Only for test coverage, It is impossible in OpenSearch.
    assertEquals(nullValue(), tupleValue("{\"not_exist\":1}").get("not_exist"));
  }

  @Test
  public void constructUnsupportedTypeThrowException() {
    OpenSearchExprValueFactory exprValueFactory =
        new OpenSearchExprValueFactory(Map.of("type", new TestType()));
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> exprValueFactory.construct("{\"type\":1}", false));
    assertEquals("Unsupported type: TEST_TYPE for value: 1.", exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class, () -> exprValueFactory.construct("type", 1, false));
    assertEquals("Unsupported type: TEST_TYPE for value: 1.", exception.getMessage());
  }

  @Test
  // aggregation adds info about new columns to the factory,
  // it is accepted without overwriting existing data.
  public void factoryMappingsAreExtendableWithoutOverWrite()
      throws NoSuchFieldException, IllegalAccessException {
    var factory = new OpenSearchExprValueFactory(Map.of("value", OpenSearchDataType.of(INTEGER)));
    factory.extendTypeMapping(
        Map.of(
            "value", OpenSearchDataType.of(DOUBLE),
            "agg", OpenSearchDataType.of(DATE)));
    // extract private field for testing purposes
    var field = factory.getClass().getDeclaredField("typeMapping");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    var mapping = (Map<String, OpenSearchDataType>) field.get(factory);
    assertAll(
        () -> assertEquals(2, mapping.size()),
        () -> assertTrue(mapping.containsKey("value")),
        () -> assertTrue(mapping.containsKey("agg")),
        () -> assertEquals(OpenSearchDataType.of(INTEGER), mapping.get("value")),
        () -> assertEquals(OpenSearchDataType.of(DATE), mapping.get("agg")));
  }

  public Map<String, ExprValue> tupleValue(String jsonString) {
    final ExprValue construct = exprValueFactory.construct(jsonString, false);
    return construct.tupleValue();
  }

  public Map<String, ExprValue> tupleValueWithArraySupport(String jsonString) {
    final ExprValue construct = exprValueFactory.construct(jsonString, true);
    return construct.tupleValue();
  }

  private ExprValue constructFromObject(String fieldName, Object value) {
    return exprValueFactory.construct(fieldName, value, false);
  }

  private ExprValue constructFromObjectWithArraySupport(String fieldName, Object value) {
    return exprValueFactory.construct(fieldName, value, true);
  }

  @EqualsAndHashCode(callSuper = false)
  @ToString
  private static class TestType extends OpenSearchDataType {

    public TestType() {
      super(MappingType.Invalid);
    }

    @Override
    protected OpenSearchDataType cloneEmpty() {
      return this;
    }

    @Override
    public String typeName() {
      return "TEST_TYPE";
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.byteValue;
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
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.StringReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprStringValue;
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
          .put("datetimeV", OpenSearchDateType.of(DATETIME))
          .put("timeV", OpenSearchDateType.of(TIME))
          .put("timestampV", OpenSearchDateType.of(TIMESTAMP))
          .put("datetimeDefaultV", OpenSearchDateType.of())
          .put("dateStringV", OpenSearchDateType.of("date"))
          .put("timeStringV", OpenSearchDateType.of("time"))
          .put("epochMillisV", OpenSearchDateType.of("epoch_millis"))
          .put("dateOrEpochMillisV", OpenSearchDateType.of("date_time_no_millis||epoch_millis"))
          .put("timeNoMillisOrTimeV", OpenSearchDateType.of("time_no_millis||time"))
          .put("dateOrOrdinalDateV", OpenSearchDateType.of("date||ordinal_date"))
          .put("customFormatV", OpenSearchDateType.of("yyyy-MM-dd-HH-mm-ss"))
          .put("customAndEpochMillisV",
              OpenSearchDateType.of("yyyy-MM-dd-HH-mm-ss||epoch_millis"))
          .put("boolV", OpenSearchDataType.of(BOOLEAN))
          .put("structV", OpenSearchDataType.of(STRUCT))
          .put("structV.id", OpenSearchDataType.of(INTEGER))
          .put("structV.state", OpenSearchDataType.of(STRING))
          .put("arrayV", OpenSearchDataType.of(ARRAY))
          .put("arrayV.info", OpenSearchDataType.of(STRING))
          .put("arrayV.author", OpenSearchDataType.of(STRING))
          .put("textV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Text))
          .put("textKeywordV", OpenSearchTextType.of(Map.of("words",
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))))
          .put("ipV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Ip))
          .put("geoV", OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint))
          .put("binaryV", OpenSearchDataType.of(OpenSearchDataType.MappingType.Binary))
          .build();

  private final OpenSearchExprValueFactory exprValueFactory =
      new OpenSearchExprValueFactory(MAPPING);

  @Test
  public void constructNullValue() {
    assertEquals(nullValue(), tupleValue("{\"intV\":null}").get("intV"));
    assertEquals(nullValue(), constructFromObject("intV",  null));
    assertTrue(new OpenSearchJsonContent(null).isNull());
  }

  @Test
  public void iterateArrayValue() throws JsonSyntaxException {
    JsonReader reader = new JsonReader(new StringReader("[\"zz\",\"bb\"]"));
    JsonParser parser = new JsonParser();
    var arrayIt = new OpenSearchJsonContent(parser.parseReader(reader)).array();
    assertTrue(arrayIt.next().stringValue().equals("zz"));
    assertTrue(arrayIt.next().stringValue().equals("bb"));
    assertTrue(!arrayIt.hasNext());
  }

  @Test
  public void iterateArrayValueWithOneElement() throws JsonSyntaxException {
    JsonReader reader = new JsonReader(new StringReader("[\"zz\"]"));
    JsonParser parser = new JsonParser();
    var arrayIt = new OpenSearchJsonContent(parser.parseReader(reader)).array();
    assertTrue(arrayIt.next().stringValue().equals("zz"));
    assertTrue(!arrayIt.hasNext());
  }

  @Test
  public void constructNullArrayValue() {
    assertEquals(nullValue(), tupleValue("{\"intV\":[]}").get("intV"));
  }

  @Test
  public void constructByte() {
    assertEquals(byteValue((byte) 1), tupleValue("{\"byteV\":1}").get("byteV"));
    assertEquals(byteValue((byte) 1), constructFromObject("byteV", 1));
    assertEquals(byteValue((byte) 1), constructFromObject("byteV", "1.0"));
  }

  @Test
  public void constructShort() {
    assertEquals(shortValue((short) 1), tupleValue("{\"shortV\":1}").get("shortV"));
    assertEquals(shortValue((short) 1), constructFromObject("shortV", 1));
    assertEquals(shortValue((short) 1), constructFromObject("shortV", "1.0"));
  }

  @Test
  public void constructInteger() {
    assertEquals(integerValue(1), tupleValue("{\"intV\":1}").get("intV"));
    assertEquals(integerValue(1), constructFromObject("intV", 1));
    assertEquals(integerValue(1), constructFromObject("intV", "1.0"));
  }

  @Test
  public void constructIntegerValueInStringValue() {
    assertEquals(integerValue(1), constructFromObject("intV", "1"));
  }

  @Test
  public void constructLong() {
    assertEquals(longValue(1L), tupleValue("{\"longV\":1}").get("longV"));
    assertEquals(longValue(1L), constructFromObject("longV", 1L));
    assertEquals(longValue(1L), constructFromObject("longV", "1.0"));
  }

  @Test
  public void constructFloat() {
    assertEquals(floatValue(1f), tupleValue("{\"floatV\":1.0}").get("floatV"));
    assertEquals(floatValue(1f), constructFromObject("floatV", 1f));
  }

  @Test
  public void constructDouble() {
    assertEquals(doubleValue(1d), tupleValue("{\"doubleV\":1.0}").get("doubleV"));
    assertEquals(doubleValue(1d), constructFromObject("doubleV", 1d));
  }

  @Test
  public void constructString() {
    assertEquals(stringValue("text"), tupleValue("{\"stringV\":\"text\"}").get("stringV"));
    assertEquals(stringValue("text"), constructFromObject("stringV", "text"));
  }

  @Test
  public void constructBoolean() {
    assertEquals(booleanValue(true), tupleValue("{\"boolV\":true}").get("boolV"));
    assertEquals(booleanValue(true), constructFromObject("boolV", true));
    assertEquals(booleanValue(true), constructFromObject("boolV", "true"));
    assertEquals(booleanValue(true), constructFromObject("boolV", 1));
    assertEquals(booleanValue(false), constructFromObject("boolV", 0));
  }

  @Test
  public void constructText() {
    assertEquals(new OpenSearchExprTextValue("text"),
                 tupleValue("{\"textV\":\"text\"}").get("textV"));
    assertEquals(new OpenSearchExprTextValue("text"),
                 constructFromObject("textV", "text"));

    assertEquals(new OpenSearchExprTextValue("text"),
                 tupleValue("{\"textKeywordV\":\"text\"}").get("textKeywordV"));
    assertEquals(new OpenSearchExprTextValue("text"),
                 constructFromObject("textKeywordV", "text"));
  }

  @Test
  public void constructDates() {
    ExprValue dateStringV = constructFromObject("dateStringV", "1984-04-12");
    assertEquals(new ExprDateValue("1984-04-12"), dateStringV);

    assertEquals(
        new ExprDateValue(LocalDate.ofInstant(Instant.ofEpochMilli(450576000000L),
            UTC_ZONE_ID)),
        constructFromObject("dateV", 450576000000L));

    assertEquals(
        new ExprDateValue("1984-04-12"),
        constructFromObject("dateOrOrdinalDateV", "1984-103"));
    assertEquals(
        new ExprDateValue("2015-01-01"),
        tupleValue("{\"dateV\":\"2015-01-01\"}").get("dateV"));
  }

  @Test
  public void constructTimes() {
    ExprValue timeStringV = constructFromObject("timeStringV","12:10:30.000Z");
    assertTrue(timeStringV.isDateTime());
    assertTrue(timeStringV instanceof ExprTimeValue);
    assertEquals(new ExprTimeValue("12:10:30"), timeStringV);

    assertEquals(
        new ExprTimeValue(LocalTime.from(Instant.ofEpochMilli(1420070400001L).atZone(UTC_ZONE_ID))),
        constructFromObject("timeV", 1420070400001L));
    assertEquals(
        new ExprTimeValue("09:07:42.000"),
        constructFromObject("timeNoMillisOrTimeV", "09:07:42.000Z"));
    assertEquals(
        new ExprTimeValue("09:07:42"),
        tupleValue("{\"timeV\":\"09:07:42\"}").get("timeV"));
  }

  @Test
  public void constructDatetime() {
    assertEquals(
        new ExprTimestampValue("2015-01-01 00:00:00"),
        tupleValue("{\"timestampV\":\"2015-01-01\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01T12:10:30Z\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01T12:10:30\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        tupleValue("{\"timestampV\":\"2015-01-01 12:10:30\"}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        tupleValue("{\"timestampV\":1420070400001}").get("timestampV"));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("timestampV", 1420070400001L));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("timestampV", Instant.ofEpochMilli(1420070400001L)));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("epochMillisV", "1420070400001"));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("epochMillisV", 1420070400001L));
    assertEquals(
        new ExprTimestampValue("2015-01-01 12:10:30"),
        constructFromObject("timestampV", "2015-01-01 12:10:30"));
    assertEquals(
        new ExprDatetimeValue("2015-01-01 12:10:30"),
        constructFromObject("datetimeV", "2015-01-01 12:10:30"));
    assertEquals(
        new ExprDatetimeValue("2015-01-01 12:10:30"),
        constructFromObject("datetimeDefaultV", "2015-01-01 12:10:30"));
    assertEquals(
        new ExprTimestampValue(Instant.ofEpochMilli(1420070400001L)),
        constructFromObject("dateOrEpochMillisV", "1420070400001"));

    // case: timestamp-formatted field, but it only gets a time: should match a time
    assertEquals(
        new ExprTimeValue("19:36:22"),
        tupleValue("{\"timestampV\":\"19:36:22\"}").get("timestampV"));

    // case: timestamp-formatted field, but it only gets a date: should match a date
    assertEquals(
        new ExprDateValue("2011-03-03"),
        tupleValue("{\"timestampV\":\"2011-03-03\"}").get("timestampV"));
  }

  @Test
  public void constructDatetime_fromCustomFormat() {
    // this is not the desirable behaviour - instead if accepts the default formatter
    assertEquals(
        new ExprDatetimeValue("2015-01-01 12:10:30"),
        constructFromObject("customFormatV", "2015-01-01 12:10:30"));

    // this should pass when custom formats are supported
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> constructFromObject("customFormatV", "2015-01-01-12-10-30"));
    assertEquals(
        "Construct ExprTimestampValue from \"2015-01-01-12-10-30\" failed, "
            + "unsupported date format.",
        exception.getMessage());

    assertEquals(
        new ExprDatetimeValue("2015-01-01 12:10:30"),
        constructFromObject("customAndEpochMillisV", "2015-01-01 12:10:30"));

    // this should pass when custom formats are supported
    exception =
        assertThrows(IllegalArgumentException.class,
            () -> constructFromObject("customAndEpochMillisV", "2015-01-01-12-10-30"));
    assertEquals(
        "Construct ExprTimestampValue from \"2015-01-01-12-10-30\" failed, "
            + "unsupported date format.",
        exception.getMessage());
  }

  @Test
  public void constructDatetimeFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> constructFromObject("timestampV", "2015-01-01 12:10"));
    assertEquals(
        "Construct ExprTimestampValue from \"2015-01-01 12:10\" failed, "
            + "unsupported date format.",
        exception.getMessage());

    // fail with missing seconds
    exception =
        assertThrows(IllegalArgumentException.class,
            () -> constructFromObject("dateOrEpochMillisV", "2015-01-01 12:10"));
    assertEquals(
        "Construct ExprTimestampValue from \"2015-01-01 12:10\" failed, "
            + "unsupported date format.",
        exception.getMessage());
  }

  @Test
  public void constructTimeFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class, () -> constructFromObject("timeV", "2015-01-01"));
    assertEquals(
        "Construct ExprTimeValue from \"2015-01-01\" failed, "
            + "unsupported time format.",
        exception.getMessage());

    exception = assertThrows(
        IllegalArgumentException.class, () -> constructFromObject("timeStringV", "10:10"));
    assertEquals(
        "Construct ExprTimeValue from \"10:10\" failed, "
            + "unsupported time format.",
        exception.getMessage());
  }

  @Test
  public void constructDateFromUnsupportedFormat_ThrowIllegalArgumentException() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class, () -> constructFromObject("dateV", "12:10:10"));
    assertEquals(
        "Construct ExprDateValue from \"12:10:10\" failed, "
            + "unsupported date format.",
        exception.getMessage());

    exception = assertThrows(
        IllegalArgumentException.class, () -> constructFromObject("dateStringV", "abc"));
    assertEquals(
        "Construct ExprDateValue from \"abc\" failed, "
            + "unsupported date format.",
        exception.getMessage());
  }

  @Test
  public void constructArray() {
    assertEquals(
        new ExprCollectionValue(ImmutableList.of(new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("info", stringValue("zz"));
                put("author", stringValue("au"));
              }
            }))),
        tupleValue("{\"arrayV\":[{\"info\":\"zz\",\"author\":\"au\"}]}").get("arrayV"));
    assertEquals(
        new ExprCollectionValue(ImmutableList.of(new ExprTupleValue(
            new LinkedHashMap<String, ExprValue>() {
              {
                put("info", stringValue("zz"));
                put("author", stringValue("au"));
              }
            }))),
        constructFromObject("arrayV", ImmutableList.of(
            ImmutableMap.of("info", "zz", "author", "au"))));
  }

  @Test
  public void constructArrayOfStrings() {
    assertEquals(new ExprCollectionValue(
            ImmutableList.of(new ExprStringValue("zz"), new ExprStringValue("au"))),
        constructFromObject("arrayV", ImmutableList.of("zz", "au")));
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
    assertEquals(new OpenSearchExprIpValue("192.168.0.1"),
        tupleValue("{\"ipV\":\"192.168.0.1\"}").get("ipV"));
  }

  @Test
  public void constructGeoPoint() {
    assertEquals(new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals(new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        tupleValue("{\"geoV\":{\"lat\":\"42.60355556\",\"lon\":\"-97.25263889\"}}").get("geoV"));
    assertEquals(new OpenSearchExprGeoPointValue(42.60355556, -97.25263889),
        constructFromObject("geoV", "42.60355556,-97.25263889"));
  }

  @Test
  public void constructGeoPointFromUnsupportedFormatShouldThrowException() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":[42.60355556,-97.25263889]}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lon\":-97.25263889}}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":-97.25263889}}").get("geoV"));
    assertEquals("geo point must in format of {\"lat\": number, \"lon\": number}",
        exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":true,\"lon\":-97.25263889}}").get("geoV"));
    assertEquals("latitude must be number value, but got value: true", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class,
            () -> tupleValue("{\"geoV\":{\"lat\":42.60355556,\"lon\":false}}").get("geoV"));
    assertEquals("longitude must be number value, but got value: false", exception.getMessage());
  }

  @Test
  public void constructBinary() {
    assertEquals(new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg=="),
        tupleValue("{\"binaryV\":\"U29tZSBiaW5hcnkgYmxvYg==\"}").get("binaryV"));
  }

  /**
   * Return the first element if is OpenSearch Array.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html.
   */
  @Test
  public void constructFromOpenSearchArrayReturnFirstElement() {
    assertEquals(integerValue(1), tupleValue("{\"intV\":[1, 2, 3]}").get("intV"));
    assertEquals(new ExprTupleValue(
        new LinkedHashMap<String, ExprValue>() {
          {
            put("id", integerValue(1));
            put("state", stringValue("WA"));
          }
        }), tupleValue("{\"structV\":[{\"id\":1,\"state\":\"WA\"},{\"id\":2,\"state\":\"CA\"}]}}")
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
        new OpenSearchExprValueFactory(ImmutableMap.of("type", new TestType()));
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> exprValueFactory.construct("{\"type\":1}"));
    assertEquals("Unsupported type: TEST_TYPE for value: 1.", exception.getMessage());

    exception =
        assertThrows(IllegalStateException.class, () -> exprValueFactory.construct("type", 1));
    assertEquals(
        "Unsupported type: TEST_TYPE for value: 1.",
        exception.getMessage());
  }

  @Test
  // aggregation adds info about new columns to the factory,
  // it is accepted without overwriting existing data.
  public void factoryMappingsAreExtendableWithoutOverWrite()
      throws NoSuchFieldException, IllegalAccessException {
    var factory = new OpenSearchExprValueFactory(Map.of("value", OpenSearchDataType.of(INTEGER)));
    factory.extendTypeMapping(Map.of(
        "value", OpenSearchDataType.of(DOUBLE),
        "agg", OpenSearchDataType.of(DATE)));
    // extract private field for testing purposes
    var field = factory.getClass().getDeclaredField("typeMapping");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    var mapping = (Map<String, OpenSearchDataType>)field.get(factory);
    assertAll(
        () -> assertEquals(2, mapping.size()),
        () -> assertTrue(mapping.containsKey("value")),
        () -> assertTrue(mapping.containsKey("agg")),
        () -> assertEquals(OpenSearchDataType.of(INTEGER), mapping.get("value")),
        () -> assertEquals(OpenSearchDataType.of(DATE), mapping.get("agg"))
    );
  }

  public Map<String, ExprValue> tupleValue(String jsonString) {
    final ExprValue construct = exprValueFactory.construct(jsonString);
    return construct.tupleValue();
  }

  private ExprValue constructFromObject(String fieldName, Object value) {
    return exprValueFactory.construct(fieldName, value);
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_HOUR_MINUTE_SECOND_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_YEAR_MONTH_DAY_FORMATTER;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchBinaryType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;
import org.opensearch.sql.opensearch.data.type.OpenSearchIpType;
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/**
 * Construct ExprValue from OpenSearch response.
 */
public class OpenSearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  private final Map<String, OpenSearchDataType> typeMapping;

  /**
   * Extend existing mapping by new data without overwrite.
   * Called from aggregation only {@link AggregationQueryBuilder#buildTypeMapping}.
   * @param typeMapping A data type mapping produced by aggregation.
   */
  public void extendTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    for (var field : typeMapping.keySet()) {
      // Prevent overwriting, because aggregation engine may be not aware
      // of all niceties of all types.
      if (!this.typeMapping.containsKey(field)) {
        this.typeMapping.put(field, typeMapping.get(field));
      }
    }
  }

  @Getter
  @Setter
  private OpenSearchAggregationResponseParser parser;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, BiFunction<Content, ExprType, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, BiFunction<Content, ExprType, ExprValue>>()
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
              (c, dt) -> new ExprIntegerValue(c.intValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Long),
              (c, dt) -> new ExprLongValue(c.longValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Short),
              (c, dt) -> new ExprShortValue(c.shortValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Byte),
              (c, dt) -> new ExprByteValue(c.byteValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Float),
              (c, dt) -> new ExprFloatValue(c.floatValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Double),
              (c, dt) -> new ExprDoubleValue(c.doubleValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
              (c, dt) -> new OpenSearchExprTextValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
              (c, dt) -> new ExprStringValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Boolean),
              (c, dt) -> ExprBooleanValue.of(c.booleanValue()))
          //Handles the creation of DATE, TIME & DATETIME
          .put(OpenSearchDateType.of(TIME),
              this::createOpenSearchDateType)
          .put(OpenSearchDateType.of(DATE),
              this::createOpenSearchDateType)
          .put(OpenSearchDateType.of(TIMESTAMP),
              this::createOpenSearchDateType)
          .put(OpenSearchDateType.of(DATETIME),
              this::createOpenSearchDateType)
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Ip),
              (c, dt) -> new OpenSearchExprIpValue(c.stringValue()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint),
              (c, dt) -> new OpenSearchExprGeoPointValue(c.geoValue().getLeft(),
                  c.geoValue().getRight()))
          .put(OpenSearchDataType.of(OpenSearchDataType.MappingType.Binary),
              (c, dt) -> new OpenSearchExprBinaryValue(c.stringValue()))
          .build();

  /**
   * Constructor of OpenSearchExprValueFactory.
   */
  public OpenSearchExprValueFactory(Map<String, OpenSearchDataType> typeMapping) {
    this.typeMapping = OpenSearchDataType.traverseAndFlatten(typeMapping);
  }

  /**
   * The struct construction has the following assumption:
   *  1. The field has OpenSearch Object data type.
   *     See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html">
   *       docs</a>
   *  2. The deeper field is flattened in the typeMapping. e.g.
   *     { "employ",       "STRUCT"  }
   *     { "employ.id",    "INTEGER" }
   *     { "employ.state", "STRING"  }
   */
  public ExprValue construct(String jsonString, boolean supportArrays) {
    try {
      return parse(new OpenSearchJsonContent(OBJECT_MAPPER.readTree(jsonString)), TOP_PATH,
          Optional.of(STRUCT), supportArrays);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("invalid json: %s.", jsonString), e);
    }
  }

  /**
   * Construct ExprValue from field and its value object. Throw exception if trying
   * to construct from field of unsupported type.
   * Todo, add IP, GeoPoint support after we have function implementation around it.
   *
   * @param field field name
   * @param value value object
   * @return ExprValue
   */
  public ExprValue construct(String field, Object value, boolean supportArrays) {
    return parse(new ObjectContent(value), field, type(field), supportArrays);
  }

  private ExprValue parse(
      Content content,
      String field,
      Optional<ExprType> fieldType,
      boolean supportArrays
  ) {
    if (content.isNull() || !fieldType.isPresent()) {
      return ExprNullValue.of();
    }

    ExprType type = fieldType.get();
    if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested))
        || content.isArray()) {
      return parseArray(content, field, type, supportArrays);
    } else if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object))
        || type == STRUCT) {
      return parseStruct(content, field, supportArrays);
    } else {
      if (typeActionMap.containsKey(type)) {
        return typeActionMap.get(type).apply(content, type);
      } else {
        throw new IllegalStateException(
            String.format(
                "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
      }
    }
  }

  /**
   * In OpenSearch, it is possible field doesn't have type definition in mapping.
   * but has empty value. For example, {"empty_field": []}.
   */
  private Optional<ExprType> type(String field) {
    return Optional.ofNullable(typeMapping.get(field));
  }

  /**
   * Parses value with the first matching formatter as an Instant to UTF.
   *
   * @param value - timestamp as string
   * @param dateType - field type
   * @return Instant without timezone
   */
  private ExprValue parseTimestampString(String value, OpenSearchDateType dateType) {
    Instant parsed = null;
    for (DateFormatter formatter : dateType.getAllNamedFormatters()) {
      try {
        TemporalAccessor accessor = formatter.parse(value);
        ZonedDateTime zonedDateTime = DateFormatters.from(accessor);
        // remove the Zone
        parsed = zonedDateTime.withZoneSameLocal(ZoneId.of("Z")).toInstant();
      } catch (IllegalArgumentException ignored) {
        // nothing to do, try another format
      }
    }

    // FOLLOW-UP PR: Check custom formatters too

    // if no named formatters are available, use the default
    if (dateType.getAllNamedFormatters().size() == 0
        || dateType.getAllCustomFormatters().size() > 0) {
      try {
        parsed = DateFormatters.from(DATE_TIME_FORMATTER.parse(value)).toInstant();
      } catch (DateTimeParseException e) {
        // ignored
      }
    }

    if (parsed == null) {
      // otherwise, throw an error that no formatters worked
      throw new IllegalArgumentException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value)
      );
    }

    return new ExprTimestampValue(parsed);
  }

  /**
   * return the first matching formatter as a time without timezone.
   *
   * @param value - time as string
   * @param dateType - field data type
   * @return time without timezone
   */
  private ExprValue parseTimeString(String value, OpenSearchDateType dateType) {
    for (DateFormatter formatter : dateType.getAllNamedFormatters()) {
      try {
        TemporalAccessor accessor = formatter.parse(value);
        ZonedDateTime zonedDateTime = DateFormatters.from(accessor);
        return new ExprTimeValue(
            zonedDateTime.withZoneSameLocal(ZoneId.of("Z")).toLocalTime());
      } catch (IllegalArgumentException  ignored) {
        // nothing to do, try another format
      }
    }

    // if no named formatters are available, use the default
    if (dateType.getAllNamedFormatters().size() == 0) {
      try {
        return new ExprTimeValue(
            DateFormatters.from(STRICT_HOUR_MINUTE_SECOND_FORMATTER.parse(value)).toLocalTime());
      } catch (DateTimeParseException e) {
        // ignored
      }
    }
    throw new IllegalArgumentException("Construct ExprTimeValue from \"" + value
        + "\" failed, unsupported time format.");
  }

  /**
   * return the first matching formatter as a date without timezone.
   *
   * @param value - date as string
   * @param dateType - field data type
   * @return date without timezone
   */
  private ExprValue parseDateString(String value, OpenSearchDateType dateType) {
    for (DateFormatter formatter : dateType.getAllNamedFormatters()) {
      try {
        TemporalAccessor accessor = formatter.parse(value);
        ZonedDateTime zonedDateTime = DateFormatters.from(accessor);
        // return the first matching formatter as a date without timezone
        return new ExprDateValue(
            zonedDateTime.withZoneSameLocal(ZoneId.of("Z")).toLocalDate());
      } catch (IllegalArgumentException  ignored) {
        // nothing to do, try another format
      }
    }

    // if no named formatters are available, use the default
    if (dateType.getAllNamedFormatters().size() == 0) {
      try {
        return new ExprDateValue(
            DateFormatters.from(STRICT_YEAR_MONTH_DAY_FORMATTER.parse(value)).toLocalDate());
      } catch (DateTimeParseException e) {
        // ignored
      }
    }
    throw new IllegalArgumentException("Construct ExprDateValue from \"" + value
        + "\" failed, unsupported date format.");
  }

  private ExprValue createOpenSearchDateType(Content value, ExprType type) {
    OpenSearchDateType dt = (OpenSearchDateType) type;
    ExprType returnFormat = dt.getExprType();

    if (value.isNumber()) {
      Instant epochMillis = Instant.ofEpochMilli(value.longValue());
      if (returnFormat == TIME) {
        return new ExprTimeValue(LocalTime.from(epochMillis.atZone(UTC_ZONE_ID)));
      }
      if (returnFormat == DATE) {
        return new ExprDateValue(LocalDate.ofInstant(epochMillis, UTC_ZONE_ID));
      }
      return new ExprTimestampValue(Instant.ofEpochMilli(value.longValue()));
    }

    if (value.isString()) {
      if (returnFormat == TIME) {
        return parseTimeString(value.stringValue(), dt);
      }
      if (returnFormat == DATE) {
        return parseDateString(value.stringValue(), dt);
      }
      // else timestamp/datetime
      return parseTimestampString(value.stringValue(), dt);
    }

    return new ExprTimestampValue((Instant) value.objectValue());
  }

  /**
   * Parse struct content.
   * @param content Content to parse.
   * @param prefix Prefix for Level of object depth to parse.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Value parsed from content.
   */
  private ExprValue parseStruct(Content content, String prefix, boolean supportArrays) {
    LinkedHashMap<String, ExprValue> result = new LinkedHashMap<>();
    content.map().forEachRemaining(entry -> result.put(entry.getKey(),
        parse(entry.getValue(),
            makeField(prefix, entry.getKey()),
            type(makeField(prefix, entry.getKey())), supportArrays)));
    return new ExprTupleValue(result);
  }

  /**
   * Parse array content. Can also parse nested which isn't necessarily an array.
   * @param content Content to parse.
   * @param prefix Prefix for Level of object depth to parse.
   * @param type Type of content parsing.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Value parsed from content.
   */
  private ExprValue parseArray(
      Content content,
      String prefix,
      ExprType type,
      boolean supportArrays
  ) {
    List<ExprValue> result = new ArrayList<>();

    // ARRAY is mapped to nested but can take the json structure of an Object.
    if (content.objectValue() instanceof ObjectNode) {
      result.add(parseStruct(content, prefix, supportArrays));
      // non-object type arrays are only supported when parsing inner_hits of OS response.
    } else if (
        !(type instanceof OpenSearchDataType
            && ((OpenSearchDataType) type).getExprType().equals(ARRAY))
        && !supportArrays) {
      return parseInnerArrayValue(content.array().next(), prefix, type, supportArrays);
    } else {
      content.array().forEachRemaining(v -> {
        result.add(parseInnerArrayValue(v, prefix, type, supportArrays));
      });
    }
    return new ExprCollectionValue(result);
  }

  /**
   * Parse inner array value. Can be object type and recurse continues.
   * @param content Array index being parsed.
   * @param prefix Prefix for value.
   * @param type Type of inner array value.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Inner array value.
   */
  private ExprValue parseInnerArrayValue(
      Content content,
      String prefix,
      ExprType type,
      boolean supportArrays
  ) {
    if (type instanceof OpenSearchIpType
        || type instanceof OpenSearchBinaryType
        || type instanceof OpenSearchDateType
        || type instanceof OpenSearchGeoPointType) {
      return parse(content, prefix, Optional.of(type), supportArrays);
    } else if (content.isString()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(STRING)), supportArrays);
    } else if (content.isLong()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(LONG)), supportArrays);
    } else if (content.isFloat()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(FLOAT)), supportArrays);
    } else if (content.isDouble()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(DOUBLE)), supportArrays);
    } else if (content.isNumber()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(INTEGER)), supportArrays);
    } else if (content.isBoolean()) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(BOOLEAN)), supportArrays);
    } else {
      return parse(content, prefix, Optional.of(STRUCT), supportArrays);
    }
  }

  /**
   * Make complete path string for field.
   * @param path Path of field.
   * @param field Field to append to path.
   * @return Field appended to path level.
   */
  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}

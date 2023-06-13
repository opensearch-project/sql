/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_HOUR_MINUTE_SECOND_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_YEAR_MONTH_DAY_FORMATTER;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader; 
import java.io.StringReader;
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
import java.util.Set;
import java.util.Map.Entry;
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
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
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
  public ExprValue construct(String jsonString) {
    try {
      JsonReader reader = new JsonReader(new StringReader(jsonString));
      JsonParser parser = new JsonParser();
      return parse(new OpenSearchJsonContent(parser.parseReader(reader)), TOP_PATH,
          Optional.of(STRUCT));
    } catch (JsonSyntaxException e) {
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
  public ExprValue construct(String field, Object value) {
    return parse(new ObjectContent(value), field, type(field));
  }

  private ExprValue parse(Content content, String field, Optional<ExprType> fieldType) {
    if (content.isNull() || !fieldType.isPresent()) {
      return ExprNullValue.of();
    }

    ExprType type = fieldType.get();
    if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object))
          || type == STRUCT) {
      return parseStruct(content, field);
    } else if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested))) {
      return parseArray(content, field);
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

  private ExprValue parseStruct(Content content, String prefix) {
    LinkedHashMap<String, ExprValue> result = new LinkedHashMap<>();
    content.map().forEachRemaining(entry -> result.put(entry.getKey(),
        parse(entry.getValue(),
            makeField(prefix, entry.getKey()),
            type(makeField(prefix, entry.getKey())))));
    return new ExprTupleValue(result);
  }

  /**
   * Todo. ARRAY is not completely supported now. In OpenSearch, there is no dedicated array type.
   * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/nested/">docs</a>
   * The similar data type is nested, but it can only allow a list of objects.
   */
  public ExprValue parseArray(Content content, String prefix) {
    List<ExprValue> result = new ArrayList<>();
    // ExprCoreType.ARRAY does not indicate inner elements type.

    try {
      if (Iterators.size(content.array()) == 1 && content.objectValue() instanceof JsonElement) {
        result.add(parse(content, prefix, Optional.of(STRUCT)));
      } else {
        content.array().forEachRemaining(v -> {
          // ExprCoreType.ARRAY does not indicate inner elements type. OpenSearch nested will be an
          // array of structs, otherwise parseArray currently only supports array of strings.
          if (v.isString()) {
            result.add(parse(v, prefix, Optional.of(OpenSearchDataType.of(STRING))));
          } else {
            result.add(parse(v, prefix, Optional.of(STRUCT)));
          }
        });
      }
    } catch (IllegalStateException e) {
      JsonObject objectValue = (JsonObject) content.objectValue();
      Set<Entry<String, JsonElement>> entrySet = objectValue.entrySet();
      if (entrySet.size() == 1) {
        result.add(parse(content, prefix, Optional.of(STRUCT)));
      } else {
        for (Entry<String, JsonElement> entry : entrySet) {
          if (entry.getValue().isJsonArray()) {
            result.add(parse(new OpenSearchJsonContent(entry.getValue()), prefix, Optional.of(OpenSearchDataType.of(MappingType.Nested))));
          }
        }
      };
    }

   

    
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}

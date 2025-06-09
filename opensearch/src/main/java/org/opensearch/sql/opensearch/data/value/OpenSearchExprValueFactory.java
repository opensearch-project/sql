/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_HOUR_MINUTE_SECOND_FORMATTER;
import static org.opensearch.sql.utils.DateTimeFormatters.STRICT_YEAR_MONTH_DAY_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.common.time.FormatNames;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchBinaryType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/** Construct ExprValue from OpenSearch response. */
public class OpenSearchExprValueFactory {
  /** The Mapping of Field and ExprType. */
  private final Map<String, OpenSearchDataType> typeMapping;

  /** Whether to support nested value types (such as arrays) */
  private final boolean fieldTypeTolerance;

  /**
   * Extend existing mapping by new data without overwrite. Called from aggregation only {@see
   * AggregationQueryBuilder#buildTypeMapping}.
   *
   * @param typeMapping A data type mapping produced by aggregation.
   */
  public void extendTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    for (var field : typeMapping.keySet()) {
      // Prevent overwriting, because aggregation engine may be not aware
      // of all niceties of all types.
      this.typeMapping.putIfAbsent(field, typeMapping.get(field));
    }
  }

  @Getter @Setter private OpenSearchAggregationResponseParser parser;

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Map<ExprType, BiFunction<Content, ExprType, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, BiFunction<Content, ExprType, ExprValue>>()
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
              (c, dt) -> new ExprIntegerValue(c.intValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Long),
              (c, dt) -> new ExprLongValue(c.longValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Short),
              (c, dt) -> new ExprShortValue(c.shortValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Byte),
              (c, dt) -> new ExprByteValue(c.byteValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Float),
              (c, dt) -> new ExprFloatValue(c.floatValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Double),
              (c, dt) -> new ExprDoubleValue(c.doubleValue()))
          .put(OpenSearchTextType.of(), (c, dt) -> new OpenSearchExprTextValue(c.stringValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
              (c, dt) -> new ExprStringValue(c.stringValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Boolean),
              (c, dt) -> ExprBooleanValue.of(c.booleanValue()))
          // Handles the creation of DATE, TIME & DATETIME
          .put(OpenSearchDateType.of(TIME), OpenSearchExprValueFactory::createOpenSearchDateType)
          .put(OpenSearchDateType.of(DATE), OpenSearchExprValueFactory::createOpenSearchDateType)
          .put(
              OpenSearchDateType.of(TIMESTAMP),
              OpenSearchExprValueFactory::createOpenSearchDateType)
          .put(
              OpenSearchDateType.of(OpenSearchDataType.MappingType.Ip),
              (c, dt) -> new ExprIpValue(c.stringValue()))
          .put(
              OpenSearchDataType.of(OpenSearchDataType.MappingType.Binary),
              (c, dt) -> new OpenSearchExprBinaryValue(c.stringValue()))
          .build();

  /** Constructor of OpenSearchExprValueFactory. */
  public OpenSearchExprValueFactory(
      Map<String, OpenSearchDataType> typeMapping, boolean fieldTypeTolerance) {
    this.typeMapping = OpenSearchDataType.traverseAndFlatten(typeMapping);
    this.fieldTypeTolerance = fieldTypeTolerance;
  }

  /**
   *
   *
   * <pre>
   * The struct construction has the following assumption:
   *  1. The field has OpenSearch Object data type.
   *     See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html">
   *       docs</a>
   *  2. The deeper field is flattened in the typeMapping. e.g.
   *     { "employ",       "STRUCT"  }
   *     { "employ.id",    "INTEGER" }
   *     { "employ.state", "STRING"  }
   *  </pre>
   */
  public ExprValue construct(String jsonString, boolean supportArrays) {
    try {
      return parse(
          new OpenSearchJsonContent(OBJECT_MAPPER.readTree(jsonString)),
          TOP_PATH,
          Optional.of(STRUCT),
          fieldTypeTolerance || supportArrays);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("invalid json: %s.", jsonString), e);
    }
  }

  /**
   * Construct ExprValue from field and its value object. Throw exception if trying to construct
   * from field of unsupported type.<br>
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
      Content content, String field, Optional<ExprType> fieldType, boolean supportArrays) {
    if (content.isNull() || !fieldType.isPresent()) {
      return ExprNullValue.of();
    }

    final ExprType type = fieldType.get();

    if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint))) {
      return parseGeoPoint(content, supportArrays);
    } else if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested))
        || content.isArray()) {
      return parseArray(content, field, type, supportArrays);
    } else if (type.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.Object))
        || type == STRUCT) {
      return parseStruct(content, field, supportArrays);
    } else if (typeActionMap.containsKey(type)) {
      return typeActionMap.get(type).apply(content, type);
    } else {
      throw new IllegalStateException(
          String.format(
              "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
    }
  }

  /**
   * In OpenSearch, it is possible field doesn't have type definition in mapping. but has empty
   * value. For example, {"empty_field": []}.
   */
  private Optional<ExprType> type(String field) {
    return Optional.ofNullable(typeMapping.get(field));
  }

  /**
   * Parse value with the first matching formatter into {@link ExprValue} with corresponding {@link
   * ExprCoreType}.
   *
   * @param value - time as string
   * @param dataType - field data type
   * @return Parsed value
   */
  private static ExprValue parseDateTimeString(String value, OpenSearchDateType dataType) {
    List<DateFormatter> formatters = dataType.getAllNamedFormatters();
    formatters.addAll(dataType.getAllCustomFormatters());
    ExprCoreType returnFormat = dataType.getExprCoreType();

    for (DateFormatter formatter : formatters) {
      try {
        TemporalAccessor accessor = formatter.parse(value);
        ZonedDateTime zonedDateTime = DateFormatters.from(accessor);
        switch (returnFormat) {
          case TIME:
            return new ExprTimeValue(zonedDateTime.withZoneSameLocal(ZoneOffset.UTC).toLocalTime());
          case DATE:
            return new ExprDateValue(zonedDateTime.withZoneSameLocal(ZoneOffset.UTC).toLocalDate());
          default:
            return new ExprTimestampValue(
                zonedDateTime.withZoneSameLocal(ZoneOffset.UTC).toInstant());
        }
      } catch (IllegalArgumentException ignored) {
        // nothing to do, try another format
      }
    }

    // if no formatters are available, try the default formatter
    try {
      switch (returnFormat) {
        case TIME:
          return new ExprTimeValue(
              DateFormatters.from(STRICT_HOUR_MINUTE_SECOND_FORMATTER.parse(value)).toLocalTime());
        case DATE:
          return new ExprDateValue(
              DateFormatters.from(STRICT_YEAR_MONTH_DAY_FORMATTER.parse(value)).toLocalDate());
        default:
          return new ExprTimestampValue(
              DateFormatters.from(DateFieldMapper.getDefaultDateTimeFormatter().parse(value))
                  .toInstant());
      }
    } catch (DateTimeParseException | IllegalArgumentException ignored) {
      // ignored
    }

    throw new IllegalArgumentException(
        String.format("Construct %s from \"%s\" failed, unsupported format.", returnFormat, value));
  }

  private static ExprValue createOpenSearchDateType(Content value, ExprType type) {
    OpenSearchDateType dt = (OpenSearchDateType) type;
    ExprCoreType returnFormat = dt.getExprCoreType();
    if (value.isNumber()) { // isNumber
      var numFormatters = dt.getNumericNamedFormatters();
      if (numFormatters.size() > 0 || !dt.hasFormats()) {
        long epochMillis = 0;
        if (numFormatters.contains(
            DateFormatter.forPattern(FormatNames.EPOCH_SECOND.getSnakeCaseName()))) {
          // no CamelCase for `EPOCH_*` formats
          epochMillis = value.longValue() * 1000;
        } else /* EPOCH_MILLIS */ {
          epochMillis = value.longValue();
        }
        Instant instant = Instant.ofEpochMilli(epochMillis);
        switch (returnFormat) {
          case TIME:
            return new ExprTimeValue(LocalTime.from(instant.atZone(ZoneOffset.UTC)));
          case DATE:
            return new ExprDateValue(LocalDate.ofInstant(instant, ZoneOffset.UTC));
          default:
            return new ExprTimestampValue(instant);
        }
      } else {
        // custom format
        return parseDateTimeString(value.objectValue().toString(), dt);
      }
    }
    if (value.isString()) {
      return parseDateTimeString(value.stringValue(), dt);
    }

    return new ExprTimestampValue((Instant) value.objectValue());
  }

  /**
   * Parse struct content.
   *
   * @param content Content to parse.
   * @param prefix Prefix for Level of object depth to parse.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Value parsed from content.
   */
  private ExprValue parseStruct(Content content, String prefix, boolean supportArrays) {
    ExprTupleValue result = ExprTupleValue.empty();
    content
        .map()
        .forEachRemaining(
            entry ->
                populateValueRecursive(
                    result,
                    new JsonPath(entry.getKey()),
                    parse(
                        entry.getValue(),
                        makeField(prefix, entry.getKey()),
                        type(makeField(prefix, entry.getKey())),
                        supportArrays)));
    return result;
  }

  /**
   * Populate the current ExprTupleValue recursively.
   *
   * <p>If JsonPath is not a root path(i.e. has dot in its raw path), it needs update to its
   * children recursively until the leaf node.
   *
   * <p>If there is existing vale for the JsonPath, we need to merge the new value to the old.
   */
  static void populateValueRecursive(ExprTupleValue result, JsonPath path, ExprValue value) {
    if (path.getPaths().size() == 1) {
      // Update the current ExprValue by using mergeTo if exists
      result
          .tupleValue()
          .computeIfPresent(path.getRootPath(), (key, oldValue) -> value.mergeTo(oldValue));
      result.tupleValue().putIfAbsent(path.getRootPath(), value);
    } else {
      result.tupleValue().putIfAbsent(path.getRootPath(), ExprTupleValue.empty());
      populateValueRecursive(
          (ExprTupleValue) result.tupleValue().get(path.getRootPath()), path.getChildPath(), value);
    }
  }

  @Getter
  static class JsonPath {
    private final List<String> paths;

    public JsonPath(String rawPath) {
      this.paths = List.of(rawPath.split("\\."));
    }

    public JsonPath(List<String> paths) {
      this.paths = paths;
    }

    public String getRootPath() {
      return paths.getFirst();
    }

    public JsonPath getChildPath() {
      return new JsonPath(paths.subList(1, paths.size()));
    }
  }

  /**
   * Parse array content. Can also parse nested which isn't necessarily an array.
   *
   * @param content Content to parse.
   * @param prefix Prefix for Level of object depth to parse.
   * @param type Type of content parsing.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Value parsed from content.
   */
  private ExprValue parseArray(
      Content content, String prefix, ExprType type, boolean supportArrays) {
    List<ExprValue> result = new ArrayList<>();

    // ARRAY is mapped to nested but can take the json structure of an Object.
    if (content.objectValue() instanceof ObjectNode) {
      result.add(parseStruct(content, prefix, supportArrays));
      // non-object type arrays are only supported when parsing inner_hits of OS response.
    } else if (!(type instanceof OpenSearchDataType
            && ((OpenSearchDataType) type).getExprType().equals(ARRAY))
        && !supportArrays) {
      return parseInnerArrayValue(content.array().next(), prefix, type, supportArrays);
    } else {
      content
          .array()
          .forEachRemaining(
              v -> {
                result.add(parseInnerArrayValue(v, prefix, type, supportArrays));
              });
    }
    return new ExprCollectionValue(result);
  }

  /**
   * Parse geo point content.
   *
   * @param content Content to parse.
   * @param supportArrays Parsing the whole array or not
   * @return Geo point value parsed from content.
   */
  private ExprValue parseGeoPoint(Content content, boolean supportArrays) {
    // there is only one point in doc.
    if (content.isArray() == false) {
      final var pair = content.geoValue();
      return new OpenSearchExprGeoPointValue(pair.getLeft(), pair.getRight());
    }

    var elements = content.array();
    var first = elements.next();
    // an array in the [longitude, latitude] format.
    if (first.isNumber()) {
      double lon = first.doubleValue();
      var second = elements.next();
      if (second.isNumber() == false) {
        throw new OpenSearchParseException("lat must be a number, got " + second.objectValue());
      }
      return new OpenSearchExprGeoPointValue(second.doubleValue(), lon);
    }

    // there are multi points in doc
    var pair = first.geoValue();
    var firstPoint = new OpenSearchExprGeoPointValue(pair.getLeft(), pair.getRight());
    if (supportArrays) {
      List<ExprValue> result = new ArrayList<>();
      result.add(firstPoint);
      elements.forEachRemaining(
          e -> {
            var p = e.geoValue();
            result.add(new OpenSearchExprGeoPointValue(p.getLeft(), p.getRight()));
          });
      return new ExprCollectionValue(result);
    } else {
      return firstPoint;
    }
  }

  /**
   * Parse inner array value. Can be object type and recurse continues.
   *
   * @param content Array index being parsed.
   * @param prefix Prefix for value.
   * @param type Type of inner array value.
   * @param supportArrays Parsing the whole array if array is type nested.
   * @return Inner array value.
   */
  private ExprValue parseInnerArrayValue(
      Content content, String prefix, ExprType type, boolean supportArrays) {
    if (type instanceof OpenSearchBinaryType || type instanceof OpenSearchDateType) {
      return parse(content, prefix, Optional.of(type), supportArrays);
    } else if (content.isString() && type.equals(OpenSearchDataType.of(IP))) {
      return parse(content, prefix, Optional.of(OpenSearchDataType.of(IP)), supportArrays);
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
   *
   * @param path Path of field.
   * @param field Field to append to path.
   * @return Field appended to path level.
   */
  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}

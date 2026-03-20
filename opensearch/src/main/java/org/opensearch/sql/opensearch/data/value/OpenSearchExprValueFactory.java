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
   * Extend existing mapping by new data. Overwrite only when the ExprCoreType of them are
   * different. Called from aggregation only {@see AggregationQueryBuilder#buildTypeMapping}.
   *
   * @param typeMapping A data type mapping produced by aggregation.
   */
  public void extendTypeMapping(Map<String, OpenSearchDataType> typeMapping) {
    typeMapping.forEach(
        (groupKey, extendedType) -> {
          OpenSearchDataType existedType = this.typeMapping.get(groupKey);
          if (existedType == null
              || !existedType.getExprCoreType().equals(extendedType.getExprCoreType())) {
            this.typeMapping.put(groupKey, extendedType);
          }
        });
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
    if (content.isNull()) {
      return ExprNullValue.of();
    }

    // Check for arrays first, even if field type is not defined in mapping.
    // This handles nested arrays in aggregation results where inner fields
    // (like sample_logs in pattern aggregation) may not have type mappings.
    // Exclude GeoPoint types as they have special array handling (e.g., [lon, lat] format).
    if (content.isArray()
        && (fieldType.isEmpty() || supportArrays)
        && !fieldType
            .map(t -> t.equals(OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint)))
            .orElse(false)) {
      ExprType type = fieldType.orElse(ARRAY);
      return parseArray(content, field, type, supportArrays);
    }

    // Field type may be not defined in mapping if users have disabled dynamic mapping.
    // Then try to parse content directly based on the value itself
    // Besides, sub-fields of generated objects are also of type UNDEFINED. We parse the content
    // directly on the value itself for this case as well.
    // TODO: Remove the second condition once https://github.com/opensearch-project/sql/issues/3751
    //  is resolved
    if (fieldType.isEmpty()
        || fieldType.get().equals(OpenSearchDataType.of(ExprCoreType.UNDEFINED))) {
      return parseContent(content);
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
      return content.isArray()
          ? parseArray(content, field, type, supportArrays)
          : typeActionMap.get(type).apply(content, type);
    } else {
      throw new IllegalStateException(
          String.format(
              "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
    }
  }

  private ExprValue parseContent(Content content) {
    if (content.isNumber()) {
      if (content.isInt()) {
        return new ExprIntegerValue(content.intValue());
      } else if (content.isShort()) {
        return new ExprShortValue(content.shortValue());
      } else if (content.isByte()) {
        return new ExprByteValue(content.byteValue());
      } else if (content.isLong()) {
        return new ExprLongValue(content.longValue());
      } else if (content.isFloat()) {
        return new ExprFloatValue(content.floatValue());
      } else if (content.isDouble()) {
        return new ExprDoubleValue(content.doubleValue());
      } else {
        // Default case for number, treat as double
        return new ExprDoubleValue(content.doubleValue());
      }
    } else if (content.isString()) {
      return new ExprStringValue(content.stringValue());
    } else if (content.isBoolean()) {
      return ExprBooleanValue.of(content.booleanValue());
    } else if (content.isNull()) {
      return ExprNullValue.of();
    }
    // Default case, treat as a string value
    return new ExprStringValue(content.objectValue().toString());
  }

  /**
   * In OpenSearch, it is possible field doesn't have type definition in mapping. but has empty
   * value. For example, {"empty_field": []}.
   */
  private Optional<ExprType> type(String field) {
    return Optional.ofNullable(typeMapping.get(field)).map(ExprType::getOriginalType);
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
    return createOpenSearchDateType(value, type, false);
  }

  private static ExprValue createOpenSearchDateType(
      Content value, ExprType type, Boolean supportArrays) {
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

    if (value.objectValue() instanceof ZonedDateTime) {
      ZonedDateTime zonedDateTime = (ZonedDateTime) value.objectValue();
      return new ExprTimestampValue(zonedDateTime.withZoneSameLocal(ZoneOffset.UTC).toInstant());
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
            entry -> {
              String fieldKey = entry.getKey();
              String fullFieldPath = makeField(prefix, fieldKey);
              // Check for malformed field names before creating JsonPath.
              // See isFieldNameMalformed() for details on what constitutes a malformed field name.
              if (isFieldNameMalformed(fieldKey)) {
                result.tupleValue().put(fieldKey, ExprNullValue.of());
              } else {
                populateValueRecursive(
                    result,
                    new JsonPath(fieldKey),
                    parse(entry.getValue(), fullFieldPath, type(fullFieldPath), supportArrays));
              }
            });
    return result;
  }

  /**
   * Check if a field name is malformed and cannot be processed by JsonPath.
   *
   * <p>A field name is malformed if it contains dot patterns that would cause String.split("\\.")
   * to produce empty strings. This includes:
   *
   * <ul>
   *   <li>Dot-only field names: ".", "..", "..."
   *   <li>Leading dots: ".a", "..a"
   *   <li>Trailing dots: "a.", "a.."
   *   <li>Consecutive dots: "a..b", "a...b"
   * </ul>
   *
   * <p>Such field names can occur in disabled object fields (enabled: false) which bypass
   * OpenSearch's field name validation. Normal OpenSearch indices reject these field names.
   *
   * @param fieldName The field name to check.
   * @return true if the field name is malformed, false otherwise.
   */
  static boolean isFieldNameMalformed(String fieldName) {
    // Use -1 limit to preserve trailing empty strings (e.g., "a." -> ["a", ""])
    String[] parts = fieldName.split("\\.", -1);
    // Dot-only field names produce empty array
    if (parts.length == 0) {
      return true;
    }
    // Check for empty parts which indicate leading, trailing, or consecutive dots
    for (String part : parts) {
      if (part.isEmpty()) {
        return true;
      }
    }
    return false;
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
    // ARRAY is mapped to nested but can take the json structure of an Object.
    if (content.objectValue() instanceof ObjectNode) {
      List<ExprValue> result = new ArrayList<>();
      result.add(parseStruct(content, prefix, supportArrays));
      return new ExprCollectionValue(result);
    }

    // Get the array iterator once and reuse it
    var arrayIterator = content.array();

    // Handle empty arrays early
    if (!arrayIterator.hasNext()) {
      return supportArrays ? new ExprCollectionValue(List.of()) : ExprNullValue.of();
    }

    // non-object type arrays are only supported when parsing inner_hits of OS response.
    if (!(type instanceof OpenSearchDataType
            && ((OpenSearchDataType) type).getExprType().equals(ARRAY))
        && !supportArrays) {
      return parseInnerArrayValue(arrayIterator.next(), prefix, type, supportArrays);
    }

    List<ExprValue> result = new ArrayList<>();
    arrayIterator.forEachRemaining(
        v -> result.add(parseInnerArrayValue(v, prefix, type, supportArrays)));
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

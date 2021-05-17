/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.data.value;

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
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_BINARY;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_GEO_POINT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_IP;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;
import static org.opensearch.sql.opensearch.data.value.OpenSearchDateFormatters.SQL_LITERAL_DATE_TIME_FORMAT;
import static org.opensearch.sql.opensearch.data.value.OpenSearchDateFormatters.STRICT_DATE_OPTIONAL_TIME_FORMATTER;
import static org.opensearch.sql.opensearch.data.value.OpenSearchDateFormatters.STRICT_HOUR_MINUTE_SECOND_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Setter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
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
import org.opensearch.sql.opensearch.data.utils.Content;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;
import org.opensearch.sql.opensearch.data.utils.OpenSearchJsonContent;

/**
 * Construct ExprValue from OpenSearch response.
 */
@AllArgsConstructor
public class OpenSearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  @Setter
  private Map<String, ExprType> typeMapping;

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(SQL_LITERAL_DATE_TIME_FORMAT)
          .appendOptional(STRICT_DATE_OPTIONAL_TIME_FORMATTER)
          .appendOptional(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
          .toFormatter();

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, Function<Content, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, Function<Content, ExprValue>>()
          .put(INTEGER, c -> new ExprIntegerValue(c.intValue()))
          .put(LONG, c -> new ExprLongValue(c.longValue()))
          .put(SHORT, c -> new ExprShortValue(c.shortValue()))
          .put(BYTE, c -> new ExprByteValue(c.byteValue()))
          .put(FLOAT, c -> new ExprFloatValue(c.floatValue()))
          .put(DOUBLE, c -> new ExprDoubleValue(c.doubleValue()))
          .put(STRING, c -> new ExprStringValue(c.stringValue()))
          .put(BOOLEAN, c -> ExprBooleanValue.of(c.booleanValue()))
          .put(TIMESTAMP, this::parseTimestamp)
          .put(DATE, c -> new ExprDateValue(parseTimestamp(c).dateValue().toString()))
          .put(TIME, c -> new ExprTimeValue(parseTimestamp(c).timeValue().toString()))
          .put(DATETIME, c -> new ExprDatetimeValue(parseTimestamp(c).datetimeValue()))
          .put(OPENSEARCH_TEXT, c -> new OpenSearchExprTextValue(c.stringValue()))
          .put(OPENSEARCH_TEXT_KEYWORD, c -> new OpenSearchExprTextKeywordValue(c.stringValue()))
          .put(OPENSEARCH_IP, c -> new OpenSearchExprIpValue(c.stringValue()))
          .put(OPENSEARCH_GEO_POINT, c -> new OpenSearchExprGeoPointValue(c.geoValue().getLeft(),
              c.geoValue().getRight()))
          .put(OPENSEARCH_BINARY, c -> new OpenSearchExprBinaryValue(c.stringValue()))
          .build();

  /**
   * The struct construction has the following assumption. 1. The field has OpenSearch Object
   * data type. https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html 2. The
   * deeper field is flattened in the typeMapping. e.g. {"employ", "STRUCT"} {"employ.id",
   * "INTEGER"} {"employ.state", "STRING"}
   */
  public ExprValue construct(String jsonString) {
    try {
      return parse(new OpenSearchJsonContent(OBJECT_MAPPER.readTree(jsonString)), TOP_PATH,
          STRUCT);
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
  public ExprValue construct(String field, Object value) {
    return parse(new ObjectContent(value), field, type(field));
  }

  private ExprValue parse(Content content, String field, ExprType type) {
    if (content.isNull()) {
      return ExprNullValue.of();
    }

    if (type == STRUCT) {
      return parseStruct(content, field);
    } else if (type == ARRAY) {
      return parseArray(content, field);
    } else {
      if (typeActionMap.containsKey(type)) {
        return typeActionMap.get(type).apply(content);
      } else {
        throw new IllegalStateException(
            String.format(
                "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
      }
    }
  }

  private ExprType type(String field) {
    if (typeMapping.containsKey(field)) {
      return typeMapping.get(field);
    } else {
      throw new IllegalStateException(String.format("No type found for field: %s.", field));
    }
  }

  /**
   * Only default strict_date_optional_time||epoch_millis is supported.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
   * The customized date_format is not supported.
   */
  private ExprValue constructTimestamp(String value) {
    try {
      return new ExprTimestampValue(
          // Using OpenSearch DateFormatters for now.
          DateFormatters.from(DATE_TIME_FORMATTER.parse(value)).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value),
          e);
    }
  }

  private ExprValue parseTimestamp(Content value) {
    if (value.isNumber()) {
      return new ExprTimestampValue(Instant.ofEpochMilli(value.longValue()));
    } else if (value.isString()) {
      return constructTimestamp(value.stringValue());
    } else {
      return new ExprTimestampValue((Instant) value.objectValue());
    }
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
   * Todo. ARRAY is not support now. In Elasticsearch, there is no dedicated array data type.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html. The similar data
   * type is nested, but it can only allow a list of objects.
   */
  private ExprValue parseArray(Content content, String prefix) {
    List<ExprValue> result = new ArrayList<>();
    content.array().forEachRemaining(v -> result.add(parse(v, prefix, STRUCT)));
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}

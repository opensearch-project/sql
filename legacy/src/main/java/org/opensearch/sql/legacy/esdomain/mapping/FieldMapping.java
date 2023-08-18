/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.esdomain.mapping;

import static java.util.Collections.emptyMap;
import static org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;

import java.util.Map;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.executor.format.DescribeResultSet;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Field mapping that parses native OpenSearch mapping.
 *
 * <p>NOTE that approaches in this class are NOT reliable because of the OpenSearch mapping query
 * API used. We should deprecate this in future and parse field mapping in more solid way.
 */
public class FieldMapping {

  /** Name of the Field to be parsed */
  private final String fieldName;

  /** Native mapping information returned from OpenSearch */
  private final Map<String, FieldMappingMetadata> typeMappings;

  /** Maps a field name to Field object that specified in query explicitly */
  private final Map<String, Field> specifiedFieldsByName;

  public FieldMapping(String fieldName) {
    this(fieldName, emptyMap(), emptyMap());
  }

  public FieldMapping(
      String fieldName,
      Map<String, FieldMappingMetadata> typeMappings,
      Map<String, Field> specifiedFieldByNames) {

    this.fieldName = fieldName;
    this.typeMappings = typeMappings;
    this.specifiedFieldsByName = specifiedFieldByNames;
  }

  /**
   * Is field specified explicitly in query
   *
   * @return true if specified
   */
  public boolean isSpecified() {
    return specifiedFieldsByName.containsKey(fieldName);
  }

  /**
   * Verify if property field matches wildcard pattern specified in query
   *
   * @return true if matched
   */
  public boolean isWildcardSpecified() {
    return specifiedFieldsByName.containsKey(path() + ".*");
  }

  /**
   * Is field a property field, which means either object field or nested field.
   *
   * @return true for property field
   */
  public boolean isPropertyField() {
    int numOfDots = StringUtils.countMatches(fieldName, '.');
    return numOfDots > 1 || (numOfDots == 1 && !isMultiField());
  }

  /**
   * Is field a/in multi-field, for example, field "a.keyword" in field "a"
   *
   * @return true for multi field
   */
  public boolean isMultiField() {
    return fieldName.endsWith(".keyword");
  }

  /**
   * Is field meta field, such as _id, _index, _source etc.
   *
   * @return true for meta field
   */
  public boolean isMetaField() {
    return fieldName.startsWith("_");
  }

  /**
   * Path of property field, for example "employee" in "employee.manager"
   *
   * @return path of property field
   */
  public String path() {
    int lastDot = fieldName.lastIndexOf(".");
    if (lastDot == -1) {
      throw new IllegalStateException(
          "path() is being invoked on the wrong field [" + fieldName + "]");
    }
    return fieldName.substring(0, lastDot);
  }

  /**
   * Find field type in OpenSearch Get Field Mapping API response. Note that Get Field Mapping API
   * does NOT return the type for object or nested field. In this case, object type is used as
   * default under the assumption that the field queried here must exist (which is true if semantic
   * analyzer is enabled).
   *
   * @return field type if found in mapping, otherwise "object" type returned
   */
  @SuppressWarnings("unchecked")
  public String type() {
    FieldMappingMetadata metaData = typeMappings.get(fieldName);
    if (metaData == null) {
      return DescribeResultSet.DEFAULT_OBJECT_DATATYPE;
    }

    Map<String, Object> source = metaData.sourceAsMap();
    String[] fieldPath = fieldName.split("\\.");

    // For object/nested field, fieldName is full path though only innermost field name present in
    // mapping
    // For example, fieldName='employee.location.city', metaData='{"city":{"type":"text"}}'
    String innermostFieldName =
        (fieldPath.length == 1) ? fieldName : fieldPath[fieldPath.length - 1];
    Map<String, Object> fieldMapping = (Map<String, Object>) source.get(innermostFieldName);
    return (String) fieldMapping.get("type");
  }
}

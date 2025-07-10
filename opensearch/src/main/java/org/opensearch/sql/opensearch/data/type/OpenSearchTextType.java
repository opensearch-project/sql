/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.data.type.ExprType;

/**
 * The type of a text value. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/text/">doc</a>
 */
public class OpenSearchTextType extends OpenSearchDataType {

  private static final OpenSearchTextType instance = new OpenSearchTextType();

  // text could have fields
  // a read-only collection
  @EqualsAndHashCode.Exclude Map<String, OpenSearchDataType> fields = ImmutableMap.of();
  @EqualsAndHashCode.Exclude private boolean fielddata = false;

  private OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = UNKNOWN;
  }

  /**
   * Constructs a Text Type using the passed in fields and fielddata argument.
   *
   * @param fields The fields to be used to construct the text type.
   * @param fielddata Whether to enable fielddata for this text type
   * @return A new OpenSeachTextTypeObject
   */
  public static OpenSearchTextType of(Map<String, OpenSearchDataType> fields, boolean fielddata) {
    var res = new OpenSearchTextType();
    res.fields = fields;
    res.fielddata = fielddata;
    return res;
  }

  /** For test only */
  public static OpenSearchTextType of(Map<String, OpenSearchDataType> fields) {
    return of(fields, false);
  }

  public static OpenSearchTextType of() {
    return OpenSearchTextType.instance;
  }

  @Override
  public List<ExprType> getParent() {
    return List.of(STRING);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  public Map<String, OpenSearchDataType> getFields() {
    return fields;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return OpenSearchTextType.of(Map.copyOf(this.fields), this.fielddata);
  }

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")<br>
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType instanceof OpenSearchTextType
        && ((OpenSearchTextType) fieldType).getFields().size() > 0) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }

  public boolean isFieldData() {
    return this.fielddata;
  }
}

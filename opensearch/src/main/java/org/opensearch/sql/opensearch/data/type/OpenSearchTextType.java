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
 * The type of a text value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/text/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchTextType extends OpenSearchDataType {

  private static final OpenSearchTextType instance = new OpenSearchTextType();

  private OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = UNKNOWN;
  }

  /**
   * Create a Text type which has fields.
   * @param fields Fields to set for the new type.
   * @return A new type object.
   */
  public static OpenSearchTextType of(Map<String, OpenSearchDataType> fields) {
    var res = new OpenSearchTextType();
    res.fields = ImmutableMap.copyOf(fields);
    return res;
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
    return OpenSearchTextType.of(fields);
  }

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType instanceof OpenSearchTextType
        && ((OpenSearchTextType) fieldType).getFields().size() > 0) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }
}

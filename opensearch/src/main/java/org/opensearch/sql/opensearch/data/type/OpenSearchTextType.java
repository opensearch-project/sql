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

  private OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = UNKNOWN;
  }

  /**
   * Constructs a Text Type using the passed in fields argument.
   *
   * @param fields The fields to be used to construct the text type.
   * @return A new OpenSeachTextTypeObject
   */
  public static OpenSearchTextType of(Map<String, OpenSearchDataType> fields) {
    var res = new OpenSearchTextType();
    res.fields = fields;
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
    return OpenSearchTextType.of(Map.copyOf(this.fields));
  }

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")<br>
   * Limitation: assume inner field name is always "keyword".
   *
   * @deprecated Use {@code toKeywordSubField(fieldName, fieldType)}
   */
  @Deprecated
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType instanceof OpenSearchTextType
        && ((OpenSearchTextType) fieldType).getFields().size() > 0) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }

  /**
   * Get the keyword subfield of the text field. Alternative of {@code
   * convertTextToKeyword(fieldName, fieldType)} in v3.
   *
   * @return the text type keyword subfield if exists, or null. If the type of filed is not text,
   *     return field name.
   */
  public static String toKeywordSubField(String fieldName, ExprType exprType) {
    if (exprType != null && exprType.getOriginalExprType() instanceof OpenSearchTextType) {
      OpenSearchTextType textType = (OpenSearchTextType) exprType.getOriginalExprType();
      // For OpenSearch Alias type which maps to the field of text type,
      // we have to use its original path
      String path = exprType.getOriginalPath().orElse(fieldName);
      // Find the first subfield with type keyword, return null if non-exist.
      return textType.getFields().entrySet().stream()
          .filter(e -> e.getValue().getMappingType() == OpenSearchDataType.MappingType.Keyword)
          .findFirst()
          .map(e -> path + "." + e.getKey())
          .orElse(null);
    } else {
      return fieldName;
    }
  }
}

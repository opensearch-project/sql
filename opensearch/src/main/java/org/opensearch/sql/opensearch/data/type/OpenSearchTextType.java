/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;

/**
 * The type of a text value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/text/">doc</a>
 */
public class OpenSearchTextType extends OpenSearchDataType {

  private static final OpenSearchTextType instance = new OpenSearchTextType();

  // text could have fields
  // a read-only collection
  @Getter
  Map<String, OpenSearchDataType> fields = ImmutableMap.of();

  private OpenSearchTextType() {
    super(MappingType.Text);
    exprCoreType = STRING;
  }

  /**
   * Constructs a Text Type using the passed in fields argument.
   * @param fields The fields to be used to construct the text type.
   * @return A new OpenSearchTextType object
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
  protected OpenSearchDataType cloneEmpty() {
    return OpenSearchTextType.of(Map.copyOf(fields));
  }

  @Override
  public String convertFieldForSearchQuery(String fieldName) {
    if (fields.size() == 0) {
      return fieldName;
    }
    // Pick first string subfield (if present) otherwise pick first subfield.
    // Multi-field text support requested in https://github.com/opensearch-project/sql/issues/1887
    String subField = fields.entrySet().stream()
        .filter(e -> e.getValue().getExprType().equals(STRING))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseGet(() -> fields.keySet().toArray(String[]::new)[0]);
    return String.format("%s.%s", fieldName, subField);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;

import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.antlr.semantic.SemanticAnalysisException;
import org.opensearch.sql.legacy.antlr.visitor.Reducible;
import org.opensearch.sql.legacy.utils.StringUtils;

/** Type interface which represents any type of symbol in the SQL. */
public interface Type extends Reducible {

  /** Hide generic type ugliness and error check here in one place. */
  @SuppressWarnings("unchecked")
  @Override
  default <T extends Reducible> T reduce(List<T> others) {
    List<Type> actualArgTypes = (List<Type>) others;
    Type result = construct(actualArgTypes);
    if (result != TYPE_ERROR) {
      return (T) result;
    }

    // Generate error message by current type name, argument types and usage of current type
    // For example, 'Function [LOG] cannot work with [TEXT, INTEGER]. Usage: LOG(NUMBER) -> NUMBER
    String actualArgTypesStr;
    if (actualArgTypes.isEmpty()) {
      actualArgTypesStr = "<None>";
    } else {
      actualArgTypesStr =
          actualArgTypes.stream().map(Type::usage).collect(Collectors.joining(", "));
    }

    throw new SemanticAnalysisException(
        StringUtils.format(
            "%s cannot work with [%s]. Usage: %s", this, actualArgTypesStr, usage()));
  }

  /**
   * Type descriptive name
   *
   * @return name
   */
  String getName();

  /**
   * Check if current type is compatible with other of same type.
   *
   * @param other other type
   * @return true if compatible
   */
  default boolean isCompatible(Type other) {
    return other == UNKNOWN || this == other;
  }

  /**
   * Construct a new type by applying current constructor on other types. Constructor is a generic
   * conception that could be function, operator, join etc.
   *
   * @param others other types
   * @return a new type as result
   */
  Type construct(List<Type> others);

  /**
   * Return typical usage of current type
   *
   * @return usage string
   */
  String usage();
}

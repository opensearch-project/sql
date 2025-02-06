/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;

/** Utility methods for handling {@link ExprValue} paths. */
@UtilityClass
public class PathUtils {

  private final Pattern PATH_SEPARATOR_PATTERN = Pattern.compile(".", Pattern.LITERAL);

  /** Returns true if a value exists at the specified path within the given root value. */
  public boolean containsExprValueAtPath(ExprValue root, String path) {
    List<String> pathComponents = splitPath(path);
    return containsExprValueForPathComponents(root, pathComponents);
  }

  /**
   * Returns the {@link ExprValue} at the specified path within the given root value. Returns {@code
   * null} if the root value does not contain the path - see {@link
   * PathUtils#containsExprValueAtPath}.
   */
  public ExprValue getExprValueAtPath(ExprValue root, String path) {

    List<String> pathComponents = splitPath(path);
    if (!containsExprValueForPathComponents(root, pathComponents)) {
      return null;
    }

    return getExprValueForPathComponents(root, pathComponents);
  }

  /**
   * Sets the {@link ExprValue} at the specified path within the given root value and returns the
   * result. Throws {@link SemanticCheckException} if the root value does not contain the path - see
   * {@link PathUtils#containsExprValueAtPath}.
   */
  public ExprValue setExprValueAtPath(ExprValue root, String path, ExprValue newValue) {

    List<String> pathComponents = splitPath(path);
    if (!containsExprValueForPathComponents(root, pathComponents)) {
      throw new SemanticCheckException(String.format("Field path '%s' does not exist.", path));
    }

    return setExprValueForPathComponents(root, pathComponents, newValue);
  }

  /** Helper method for {@link PathUtils#containsExprValueAtPath}. */
  private boolean containsExprValueForPathComponents(ExprValue root, List<String> pathComponents) {

    if (pathComponents.isEmpty()) {
      return true;
    }

    if (!root.type().equals(STRUCT)) {
      return false;
    }

    String currentPathComponent = pathComponents.getFirst();
    List<String> remainingPathComponents = pathComponents.subList(1, pathComponents.size());

    Map<String, ExprValue> exprValueMap = root.tupleValue();
    if (!exprValueMap.containsKey(currentPathComponent)) {
      return false;
    }

    return containsExprValueForPathComponents(
        exprValueMap.get(currentPathComponent), remainingPathComponents);
  }

  /** Helper method for {@link PathUtils#getExprValueAtPath}. */
  private ExprValue getExprValueForPathComponents(ExprValue root, List<String> pathComponents) {

    if (pathComponents.isEmpty()) {
      return root;
    }

    String currentPathComponent = pathComponents.getFirst();
    List<String> remainingPathComponents = pathComponents.subList(1, pathComponents.size());

    Map<String, ExprValue> exprValueMap = root.tupleValue();
    return getExprValueForPathComponents(
        exprValueMap.get(currentPathComponent), remainingPathComponents);
  }

  /** Helper method for {@link PathUtils#setExprValueAtPath}. */
  private ExprValue setExprValueForPathComponents(
      ExprValue root, List<String> pathComponents, ExprValue newValue) {

    if (pathComponents.isEmpty()) {
      return newValue;
    }

    String currentPathComponent = pathComponents.getFirst();
    List<String> remainingPathComponents = pathComponents.subList(1, pathComponents.size());

    Map<String, ExprValue> exprValueMap = new HashMap<>(root.tupleValue());
    exprValueMap.put(
        currentPathComponent,
        setExprValueForPathComponents(
            exprValueMap.get(currentPathComponent), remainingPathComponents, newValue));

    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }

  /** Splits the given path and returns the corresponding components. */
  private List<String> splitPath(String path) {
    return Arrays.asList(PATH_SEPARATOR_PATTERN.split(path));
  }
}

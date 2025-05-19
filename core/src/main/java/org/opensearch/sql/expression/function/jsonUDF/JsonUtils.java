/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;

public class JsonUtils {
  static ObjectMapper objectMapper = new ObjectMapper();
  public static Gson gson = new Gson();
  public static String MEANING_LESS_KEY_FOR_APPEND_AND_EXTEND = ".meaningless_key";

  /**
   * @param input candidate json path like a.b{}.c{2}
   * @return the normalized json path like $.a.b[*].c[2]
   */
  public static String convertToJsonPath(String input) {
    if (input == null || input.isEmpty()) return "$";

    StringBuilder sb = new StringBuilder("$.");
    int i = 0;
    while (i < input.length()) {
      char c = input.charAt(i);

      if (c == '{') {
        int end = input.indexOf('}', i);
        if (end == -1) throw new IllegalArgumentException("Unmatched { in input");

        String index = input.substring(i + 1, end).trim();
        if (index.isEmpty()) {
          sb.append("[*]");
        } else {
          sb.append("[").append(index).append("]");
        }
        i = end + 1;
      } else if (c == '.') {
        sb.append(".");
        i++;
      } else {
        int start = i;
        while (i < input.length() && input.charAt(i) != '.' && input.charAt(i) != '{') {
          i++;
        }
        sb.append(input, start, i);
      }
    }

    return sb.toString();
  }

  /**
   * Transfer the object input to json node
   *
   * @param input
   * @return
   */
  public static JsonNode convertInputToJsonNode(Object input) {
    try {
      JsonNode root;
      if (input instanceof String) {
        root = objectMapper.readTree(input.toString());
      } else {
        root = objectMapper.valueToTree(input);
      }
      return root;
    } catch (Exception e) {
      throw new RuntimeException("fail to parse input", e);
    }
  }

  /**
   * The function will expand the json path to eliminate the wildcard *. For example, a[*] would be
   * a[0], a[1]...
   *
   * @param root The json node
   * @param rawPath original path
   * @return List of expanded paths
   */
  public static List<String> expandJsonPath(JsonNode root, String rawPath) {
    // Remove only leading "$." or "$"
    String cleanedPath = rawPath.replaceFirst("^\\$\\.", "").replaceFirst("^\\$", "");

    String[] parts = cleanedPath.split("\\.");
    return expand(root, parts, 0, "$");
  }

  private static List<String> expand(
      JsonNode currentNode, String[] parts, int index, String prefix) {
    if (index >= parts.length || currentNode == null) {
      return List.of(prefix);
    }

    String part = parts[index];
    List<String> results = new ArrayList<>();

    if (part.endsWith("[*]")) { // Contains wildcard symbol
      String field = part.substring(0, part.length() - 3);
      JsonNode arrayNode;
      if (field.isEmpty()) {
        arrayNode = currentNode;
      } else {
        arrayNode = currentNode.get(field);
      }
      if (arrayNode != null && arrayNode.isArray()) {
        for (int i = 0; i < arrayNode.size(); i++) {
          String newPrefix = prefix + "." + field + "[" + i + "]";
          results.addAll(expand(arrayNode.get(i), parts, index + 1, newPrefix));
        }
      }
    } else if (part.endsWith("]")) { // Normal index symbol
      int leftBracketIndex = part.lastIndexOf('[');
      String field = part.substring(0, part.length() - 3);
      JsonNode arrayNode;
      if (field.isEmpty()) {
        arrayNode = currentNode;
      } else {
        arrayNode = currentNode.get(field);
      }
      Boolean arrayFlag = false;
      if (leftBracketIndex > -1
          && part.substring(leftBracketIndex + 1, part.length() - 1).matches("-?\\d+")) {
        int currentIndex =
            Integer.parseInt(part.substring(leftBracketIndex + 1, part.length() - 1));
        if (arrayNode != null && arrayNode.isArray()) {
          if (arrayNode.size() > currentIndex) {
            String newPrefix = prefix + "." + part;
            results.addAll(expand(arrayNode.get(currentIndex), parts, index + 1, newPrefix));
            arrayFlag = true;
          }
        }
      }
      if (!arrayFlag) { // normal keys
        JsonNode next = currentNode.get(part);
        String newPrefix = prefix + "." + part;
        results.addAll(expand(next, parts, index + 1, newPrefix));
      }
    } else { // ends with string
      JsonNode next = currentNode.get(part);
      String newPrefix = prefix + "." + part;
      results.addAll(expand(next, parts, index + 1, newPrefix));
    }

    return results;
  }
}

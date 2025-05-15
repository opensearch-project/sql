/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonUtils {
  static ObjectMapper objectMapper = new ObjectMapper();

  static Object parseValue(String value) {
    // Try parsing the value as JSON, fallback to primitive if parsing fails
    try {
      return objectMapper.readValue(value, Object.class);
    } catch (Exception e) {
      // Primitive value, return as is
      return value;
    }
  }

  @FunctionalInterface
  interface UpdateConsumer {
    void apply(Map<String, Object> obj, String key, Object value);
  }

  private static void traverseNestedObject(
      Object currentObj,
      String[] pathParts,
      int depth,
      Object valueToUpdate,
      UpdateConsumer updateObjectFunction) {
    if (currentObj == null || depth >= pathParts.length) {
      return;
    }

    if (currentObj instanceof Map) {
      Map<String, Object> currentMap = (Map<String, Object>) currentObj;
      String currentKey = pathParts[depth];

      if (depth == pathParts.length - 1) {
        updateObjectFunction.apply(currentMap, currentKey, valueToUpdate);
      } else {
        // Continue traversing
        currentMap.computeIfAbsent(
            currentKey, k -> new LinkedHashMap<>()); // Create map if not present
        traverseNestedObject(
            currentMap.get(currentKey), pathParts, depth + 1, valueToUpdate, updateObjectFunction);
      }
    } else if (currentObj instanceof List) {
      // If the current object is a list, process each map in the list
      List<Object> list = (List<Object>) currentObj;
      for (Object item : list) {
        if (item instanceof Map) {
          traverseNestedObject(item, pathParts, depth, valueToUpdate, updateObjectFunction);
        }
      }
    }
  }

  static String updateNestedJson(
      String jsonStr, List<String> pathValues, UpdateConsumer updateFieldConsumer) {
    if (jsonStr == null) {
      return null;
    }
    // don't update if the list is empty, or the list is not key-value pairs
    if (pathValues.isEmpty()) {
      return jsonStr;
    }
    try {
      // Parse the JSON string into a Map
      Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, Map.class);

      // Iterate through the key-value pairs and update the json
      var iter = pathValues.iterator();
      while (iter.hasNext()) {
        String path = iter.next();
        if (!iter.hasNext()) {
          // no value provided and cannot update anything
          break;
        }
        String[] pathParts = path.split("\\.");
        Object parsedValue = parseValue(iter.next());

        traverseNestedObject(jsonMap, pathParts, 0, parsedValue, updateFieldConsumer);
      }

      // Convert the updated map back to JSON
      return objectMapper.writeValueAsString(jsonMap);
    } catch (Exception e) {
      return null;
    }
  }

  static void appendObjectValue(Map<String, Object> obj, String key, Object value) {
    // If it's the last key, append to the array
    obj.computeIfAbsent(key, k -> new ArrayList<>()); // Create list if not present
    Object existingValue = obj.get(key);

    if (existingValue instanceof List) {
      List<Object> list = (List<Object>) existingValue;
      list.add(value);
    }
  }

  static void extendObjectValue(Map<String, Object> obj, String key, Object value) {
    // If it's the last key, append to the array
    obj.computeIfAbsent(key, k -> new ArrayList<>()); // Create list if not present
    Object existingValue = obj.get(key);

    if (existingValue instanceof List) {
      List<Object> existingList = (List<Object>) existingValue;
      if (value instanceof List) {
        existingList.addAll((List) value);
      } else {
        existingList.add(value);
      }
    }
  }

  /**
   * remove nested json object using its keys parts.
   *
   * @param currentObj
   * @param keyParts
   * @param depth
   */
  static void removeNestedKey(Object currentObj, String[] keyParts, int depth) {
    if (currentObj == null || depth >= keyParts.length) {
      return;
    }

    if (currentObj instanceof Map) {
      Map<String, Object> currentMap = (Map<String, Object>) currentObj;
      String currentKey = keyParts[depth];

      if (depth == keyParts.length - 1) {
        // If it's the last key, remove it from the map
        currentMap.remove(currentKey);
      } else {
        // If not the last key, continue traversing
        if (currentMap.containsKey(currentKey)) {
          Object nextObj = currentMap.get(currentKey);

          if (nextObj instanceof List) {
            // If the value is a list, process each item in the list
            List<Object> list = (List<Object>) nextObj;
            for (int i = 0; i < list.size(); i++) {
              removeNestedKey(list.get(i), keyParts, depth + 1);
            }
          } else {
            // Continue traversing if it's a map
            removeNestedKey(nextObj, keyParts, depth + 1);
          }
        }
      }
    }
  }

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
        // 处理 {...} 为数组访问或通配符
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
        // 读取字段名
        int start = i;
        while (i < input.length() && input.charAt(i) != '.' && input.charAt(i) != '{') {
          i++;
        }
        sb.append(input, start, i);
      }
    }

    return sb.toString();
  }

  static JsonNode deletePath(JsonNode node, PathTokenizer tokenizer) {
    if (!tokenizer.hasNext()) return node;

    PathToken token = tokenizer.next();

    if (token.key.equals("$")) {
      return deletePath(node, tokenizer);
    }
    if (node instanceof ArrayNode arr && token.isIndex) {
      if (token.key.equals("*")) {
        for (int i = arr.size() - 1; i >= 0; i--) {
          deletePath(arr.get(i), tokenizer.cloneFromNext());
        }
      } else {
        int idx = Integer.parseInt(token.key);
        if (!tokenizer.hasNext()) {
          if (idx >= 0 && idx < arr.size()) arr.remove(idx);
        } else {
          if (idx >= 0 && idx < arr.size()) {
            deletePath(arr.get(idx), tokenizer);
          }
        }
      }
    } else if (node instanceof ObjectNode obj && !token.isIndex) {
      if (!tokenizer.hasNext()) {
        obj.remove(token.key);
      } else if (obj.has(token.key)) {
        deletePath(obj.get(token.key), tokenizer);
      }
    }

    return node;
  }

  // Tokenizer for JSONPath like $[0].cities[1]
  public static class PathTokenizer {
    private final List<PathToken> tokens;
    private int index = 0;

    public PathTokenizer(String path) {
      this.tokens = new ArrayList<>();

      // normalize brackets (a[1] => a.[1])
      String normalized = path.replaceAll("\\[", ".[");

      for (String raw : normalized.split("\\.")) {
        if (raw.isEmpty()) continue;

        if (raw.startsWith("[") && raw.endsWith("]")) {
          tokens.add(new PathToken(raw.substring(1, raw.length() - 1), true));
        } else {
          tokens.add(new PathToken(raw, false));
        }
      }
    }

    public boolean hasNext() {
      return index < tokens.size();
    }

    public PathToken next() {
      return tokens.get(index++);
    }

    public PathTokenizer cloneFromNext() {
      PathTokenizer clone = new PathTokenizer("");
      clone.tokens.addAll(this.tokens);
      clone.index = this.index;
      return clone;
    }
  }

  static class PathToken {
    public final String key;
    public final boolean isIndex;

    public PathToken(String key, boolean isIndex) {
      this.key = key;
      this.isIndex = isIndex;
    }
  }

  public static JsonNode verifyInput(Object input) {
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
}

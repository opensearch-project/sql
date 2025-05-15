/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;

public class JsonUtils {
  static ObjectMapper objectMapper = new ObjectMapper();
  static Gson gson = new Gson();

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

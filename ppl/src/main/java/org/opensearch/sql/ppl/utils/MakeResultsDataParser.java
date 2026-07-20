/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprCoreType;

/**
 * Parses the inline {@code makeresults format=csv|json data="..."} literal into a shared {@link
 * Values} node of typed literal rows. JSON values infer their type (integer to long, decimal to
 * float, boolean, string); a CSV {@code name:type} header declares the type, a bare name is string.
 * UDT types (timestamp/date/time/ip/json) are not yet supported on this path.
 */
public final class MakeResultsDataParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private MakeResultsDataParser() {}

  public static Values parse(String format, String data) {
    String fmt = format == null ? null : format.toLowerCase(Locale.ROOT);
    Values result;
    if ("json".equals(fmt)) {
      result = parseJson(data);
    } else if ("csv".equals(fmt)) {
      result = parseCsv(data);
    } else {
      throw new SyntaxCheckException("makeresults format must be 'csv' or 'json'");
    }
    // Cap inline cells (rows x columns): the generated literal method hits the JVM 64 KB
    // per-method bytecode limit above ~6900 cells, so a flat row cap is unsafe for wide data.
    if (result.getValues() != null && !result.getValues().isEmpty()) {
      int rows = result.getValues().size();
      int cols = result.getValues().get(0).size();
      long cells = (long) rows * cols;
      if (cells > 5000) {
        throw new SyntaxCheckException(
            "makeresults data must not exceed 5000 cells (rows x columns); got "
                + rows
                + " rows x "
                + cols
                + " columns = "
                + cells);
      }
      // A single string literal must fit the 65535-byte constant-pool CONSTANT_Utf8 limit.
      for (List<Literal> row : result.getValues()) {
        for (Literal cell : row) {
          Object v = cell.getValue();
          if (v instanceof String && ((String) v).length() > 60000) {
            throw new SyntaxCheckException(
                "makeresults data cell value must not exceed 60000 characters; got "
                    + ((String) v).length());
          }
        }
      }
    }
    return result;
  }

  private static Values toValues(
      List<String> names,
      List<ExprCoreType> types,
      List<List<Object>> rows,
      boolean withImplicitTimestamp) {
    List<DataType> dataTypes = new ArrayList<>();
    for (ExprCoreType t : types) {
      dataTypes.add(exprToDataType(t));
    }
    List<List<Literal>> literalRows = new ArrayList<>();
    for (List<Object> row : rows) {
      List<Literal> out = new ArrayList<>();
      for (int i = 0; i < names.size(); i++) {
        Object v = row.get(i);
        out.add(v == null ? new Literal(null, DataType.NULL) : new Literal(v, dataTypes.get(i)));
      }
      literalRows.add(out);
    }
    return new Values(literalRows, names, types, withImplicitTimestamp);
  }

  private static DataType exprToDataType(ExprCoreType t) {
    switch (t) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INTEGER:
        return DataType.INTEGER;
      case LONG:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case STRING:
      default:
        return DataType.STRING;
    }
  }

  private static Values parseJson(String data) {
    JsonNode arr;
    try {
      arr = MAPPER.readTree(data);
    } catch (Exception e) {
      throw new SyntaxCheckException("makeresults data is not valid JSON: " + e.getMessage());
    }
    if (arr == null || !arr.isArray()) {
      throw new SyntaxCheckException("makeresults JSON data must be an array of objects");
    }
    LinkedHashMap<String, ExprCoreType> cols = new LinkedHashMap<>();
    List<Map<String, Object>> raw = new ArrayList<>();
    for (JsonNode node : arr) {
      if (!node.isObject()) {
        throw new SyntaxCheckException("makeresults JSON data must be an array of objects");
      }
      Map<String, Object> row = new LinkedHashMap<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> f = it.next();
        String name = f.getKey();
        JsonNode v = f.getValue();
        ExprCoreType inferred = inferJsonType(v);
        if (inferred == null) {
          cols.putIfAbsent(name, null);
        } else {
          cols.merge(name, inferred, MakeResultsDataParser::widen);
        }
        row.put(name, jsonValue(v));
      }
      raw.add(row);
    }
    List<String> names = new ArrayList<>(cols.keySet());
    List<ExprCoreType> types = new ArrayList<>();
    for (String n : names) {
      ExprCoreType t = cols.get(n);
      if (t == null) {
        throw new SyntaxCheckException(
            "makeresults column '"
                + n
                + "' has only null values; provide at least one non-null"
                + " value so its type can be determined");
      }
      types.add(t);
    }
    List<List<Object>> rows = new ArrayList<>();
    for (Map<String, Object> r : raw) {
      List<Object> out = new ArrayList<>();
      for (int i = 0; i < names.size(); i++) {
        out.add(coerce(r.get(names.get(i)), types.get(i)));
      }
      rows.add(out);
    }
    return toValues(names, types, rows, true);
  }

  private static Values parseCsv(String data) {
    String[] lines = data.split("\r?\n", -1);
    if (lines.length == 0 || lines[0].trim().isEmpty()) {
      throw new SyntaxCheckException("makeresults CSV data must start with a header line");
    }
    String[] header = splitCsvLine(lines[0]).toArray(new String[0]);
    List<String> names = new ArrayList<>();
    List<ExprCoreType> types = new ArrayList<>();
    for (String token : header) {
      String t = token.trim();
      String name = t;
      ExprCoreType type = ExprCoreType.STRING;
      int colon = t.lastIndexOf(':');
      if (colon > 0 && colon < t.length() - 1) {
        ExprCoreType declared = resolveType(t.substring(colon + 1).trim());
        if (declared != null) {
          name = t.substring(0, colon).trim();
          type = declared;
        }
      }
      if (name.isEmpty()) {
        throw new SyntaxCheckException(
            "makeresults CSV header has a blank column name: " + lines[0]);
      }
      names.add(name);
      types.add(type);
    }
    names = uniquify(names);
    List<List<Object>> rows = new ArrayList<>();
    for (int li = 1; li < lines.length; li++) {
      if (lines[li].trim().isEmpty()) {
        continue;
      }
      List<String> cells = splitCsvLine(lines[li]);
      if (cells.size() > names.size()) {
        throw new SyntaxCheckException(
            "makeresults CSV row has more columns than the header: " + lines[li]);
      }
      List<Object> out = new ArrayList<>();
      for (int i = 0; i < names.size(); i++) {
        String cell = i < cells.size() ? cells.get(i).trim() : "";
        out.add(coerce(cell.isEmpty() ? null : cell, types.get(i)));
      }
      rows.add(out);
    }
    return toValues(names, types, rows, false);
  }

  private static List<String> splitCsvLine(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
      } else if (c == ',') {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    if (inQuotes) {
      throw new SyntaxCheckException(
          "makeresults CSV data has an unterminated quoted field: " + line);
    }
    out.add(cur.toString());
    return out;
  }

  private static List<String> uniquify(List<String> names) {
    List<String> out = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (String n : names) {
      String candidate = n;
      int suffix = 0;
      while (!seen.add(candidate)) {
        candidate = n + suffix++;
      }
      out.add(candidate);
    }
    return out;
  }

  private static ExprCoreType inferJsonType(JsonNode v) {
    if (v.isNull()) {
      return null;
    }
    if (v.isObject() || v.isArray()) {
      return ExprCoreType.STRING;
    }
    if (v.isBoolean()) {
      return ExprCoreType.BOOLEAN;
    }
    if (v.isIntegralNumber()) {
      // A JSON integer wider than long keeps full precision as a string rather than overflowing.
      return v.canConvertToLong() ? ExprCoreType.LONG : ExprCoreType.STRING;
    }
    if (v.isNumber()) {
      return ExprCoreType.FLOAT;
    }
    return ExprCoreType.STRING;
  }

  private static ExprCoreType widen(ExprCoreType a, ExprCoreType b) {
    if (a == null) {
      return b;
    }
    if (b == null || a == b) {
      return a;
    }
    boolean an = a == ExprCoreType.LONG || a == ExprCoreType.FLOAT;
    boolean bn = b == ExprCoreType.LONG || b == ExprCoreType.FLOAT;
    if (an && bn) {
      return ExprCoreType.FLOAT;
    }
    return ExprCoreType.STRING;
  }

  private static Object jsonValue(JsonNode v) {
    if (v.isNull()) {
      return null;
    }
    if (v.isObject() || v.isArray()) {
      return v.toString();
    }
    if (v.isBoolean()) {
      return v.booleanValue();
    }
    if (v.isIntegralNumber()) {
      return v.canConvertToLong() ? v.longValue() : v.asText();
    }
    if (v.isNumber()) {
      return v.doubleValue();
    }
    return v.asText();
  }

  private static ExprCoreType resolveType(String name) {
    switch (name.toLowerCase(Locale.ROOT)) {
      case "string":
        return ExprCoreType.STRING;
      case "boolean":
        return ExprCoreType.BOOLEAN;
      case "int":
      case "integer":
        return ExprCoreType.INTEGER;
      case "long":
        return ExprCoreType.LONG;
      case "float":
        return ExprCoreType.FLOAT;
      case "double":
        return ExprCoreType.DOUBLE;
      case "date":
      case "time":
      case "timestamp":
      case "ip":
      case "json":
        throw new SyntaxCheckException(
            "makeresults inline type '" + name + "' is not yet supported; use string and cast");
      default:
        return null;
    }
  }

  private static Object coerce(Object value, ExprCoreType type) {
    if (value == null) {
      return null;
    }
    String s = String.valueOf(value);
    try {
      switch (type) {
        case STRING:
          return s;
        case BOOLEAN:
          return value instanceof Boolean ? value : parseBooleanStrict(s.trim());
        case INTEGER:
          return Integer.parseInt(s.trim());
        case LONG:
          return value instanceof Long ? value : Long.parseLong(s.trim());
        case FLOAT:
        case DOUBLE:
          return value instanceof Double ? value : Double.parseDouble(s.trim());
        default:
          return s;
      }
    } catch (NumberFormatException e) {
      throw new SyntaxCheckException(
          "makeresults cannot parse \"" + s + "\" as " + type.typeName());
    }
  }

  private static Boolean parseBooleanStrict(String s) {
    if ("true".equalsIgnoreCase(s)) {
      return Boolean.TRUE;
    }
    if ("false".equalsIgnoreCase(s)) {
      return Boolean.FALSE;
    }
    throw new SyntaxCheckException(
        "makeresults cannot parse \"" + s + "\" as boolean; expected true or false");
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.explain;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/** Explain query plan in JSON format. */
public class JsonExplanationFormat implements ExplanationFormat {

  /** JSONObject stack to track the path from root to current ndoe */
  private final Deque<JSONObject> jsonObjStack = new ArrayDeque<>();

  /** Indentation in final output string */
  private final int indent;

  public JsonExplanationFormat(int indent) {
    this.indent = indent;
  }

  @Override
  public void prepare(Map<String, String> kvs) {
    jsonObjStack.push(new JSONObject(kvs));
  }

  @Override
  public void start(String name) {
    JSONObject json = new JSONObject();
    jsonObjStack.peek().put(name, json);
    jsonObjStack.push(json);
  }

  @Override
  public void explain(Object obj) {
    JSONObject json = new JSONObject(obj); // JSONify using getter
    jsonifyValueIfValidJson(json);
    appendToArrayIfExist(nodeName(obj), json);
    jsonObjStack.push(json);
  }

  @Override
  public void end() {
    jsonObjStack.pop();
  }

  @Override
  public String toString() {
    return jsonObjStack.pop().toString(indent);
  }

  /**
   * Trick to parse JSON in field getter due to missing support for custom processor in org.json.
   * And also because it's not appropriate to make getter aware of concrete format logic
   */
  private void jsonifyValueIfValidJson(JSONObject json) {
    for (String key : json.keySet()) {
      try {
        JSONObject jsonValue = new JSONObject(json.getString(key));
        json.put(key, jsonValue);
      } catch (JSONException e) {
        // Ignore value that is not a valid JSON.
      }
    }
  }

  private String nodeName(Object obj) {
    return obj.toString(); // obj.getClass().getSimpleName();
  }

  /** Replace JSONObject by JSONArray if key is duplicate */
  private void appendToArrayIfExist(String name, JSONObject child) {
    JSONObject parent = jsonObjStack.peek();
    Object otherChild = parent.opt(name);
    if (otherChild == null) {
      parent.put(name, child);
    } else {
      if (!(otherChild instanceof JSONArray)) {
        parent.remove(name);
        parent.append(name, otherChild);
      }
      parent.append(name, child);
    }
  }
}

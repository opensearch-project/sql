/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.UnsupportedScriptException;

@Getter
public class ScriptParameterHelper {
  public static final String MISSING_MAX = "MISSING_MAX";

  public static final String SOURCES = "SOURCES";
  public static final String DIGESTS = "DIGESTS";

  /** Existing params derived from push down action, e.g., SORT_EXPR push down */
  Map<String, Object> existingParams;

  List<RelDataTypeField> inputFieldList;
  Map<String, ExprType> fieldTypes;
  RexBuilder rexBuilder;

  /** The stack to record whether the current RexCall allows widening numeric type */
  final Deque<Boolean> stack = new ArrayDeque<>();

  /**
   * Records the source of each parameter, it decides which kind of source to retrieve value.
   *
   * <p>0 stands for DOC_VALUE
   *
   * <p>1 stands for SOURCE
   *
   * <p>2 stands for LITERAL
   *
   * <p>3 stands for SPECIAL_VARIABLE - retrieves value from special context variables (e.g., state,
   * states in scripted metric aggregations)
   */
  List<Integer> sources;

  /**
   * Records the digest of each parameter, which is used as the key to retrieve the value from the
   * corresponding sources. It will be
   *
   * <p>- field name for `DOC_VALUE` and `SOURCE`,
   *
   * <p>- literal value itself for `LITERAL`
   */
  List<Object> digests;

  public ScriptParameterHelper(
      List<RelDataTypeField> inputFieldList,
      Map<String, ExprType> fieldTypes,
      RexBuilder rexBuilder) {
    this(inputFieldList, fieldTypes, Collections.emptyMap(), rexBuilder);
  }

  public ScriptParameterHelper(
      List<RelDataTypeField> inputFieldList,
      Map<String, ExprType> fieldTypes,
      Map<String, Object> params,
      RexBuilder rexBuilder) {
    this.existingParams = params;
    this.inputFieldList = inputFieldList;
    this.fieldTypes = fieldTypes;
    this.rexBuilder = rexBuilder;
    this.sources = new ArrayList<>();
    this.digests = new ArrayList<>();
  }

  public Map<String, Object> getParameters() {
    //  The timestamp when the query is executed, it's used by some time-related functions.
    long currentTime = Hook.CURRENT_TIME.get(-1L);
    if (currentTime < 0) {
      throw new UnsupportedScriptException(
          "ScriptQueryExpression requires a valid current time from hook, but it is not set");
    }
    return new LinkedHashMap<>() { // Use LinkedHashMap to make the plan stable
      {
        putAll(existingParams);
        put(Variable.UTC_TIMESTAMP.camelName, currentTime);
        put(SOURCES, sources);
        put(DIGESTS, digests);
      }
    };
  }

  /**
   * Adds a special variable reference (like state or states in scripted metric aggregations) and
   * returns the index.
   *
   * @param variableName The name of the special variable (e.g., "state", "states")
   * @return The index in the sources/digests lists
   */
  public int addSpecialVariable(String variableName) {
    int index = sources.size();
    sources.add(3); // SPECIAL_VARIABLE = 3
    digests.add(variableName);
    return index;
  }
}

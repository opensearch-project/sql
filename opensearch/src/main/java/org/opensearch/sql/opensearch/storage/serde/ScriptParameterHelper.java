/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.UnsupportedScriptException;

@Getter
public class ScriptParameterHelper {
  public static final String SOURCES = "SOURCES";
  public static final String DIGESTS = "DIGESTS";
  public static final String LITERALS = "LITERALS";

  List<RelDataTypeField> inputFieldList;
  Map<String, ExprType> fieldTypes;
  int[] currentIndex;
  List<Integer> sources;
  List<Object> digests;
  List<Object> literals;

  public ScriptParameterHelper(
      List<RelDataTypeField> inputFieldList, Map<String, ExprType> fieldTypes) {
    this.inputFieldList = inputFieldList;
    this.fieldTypes = fieldTypes;
    this.currentIndex = new int[] {0};
    this.sources = new ArrayList<>();
    this.digests = new ArrayList<>();
    this.literals = new ArrayList<>();
  }

  public Map<String, Object> getParameters() {
    long currentTime = Hook.CURRENT_TIME.get(-1L);
    if (currentTime < 0) {
      throw new UnsupportedScriptException(
          "ScriptQueryExpression requires a valid current time from hook, but it is not set");
    }
    return new LinkedHashMap<>() { // Use LinkedHashMap to make the plan stable
      {
        put(Variable.UTC_TIMESTAMP.camelName, currentTime);
        put(SOURCES, sources);
        put(DIGESTS, digests);
        put(LITERALS, literals);
      }
    };
  }
}

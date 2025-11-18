/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import java.util.ArrayList;
import java.util.Collections;
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
  public static final String MISSING_MAX = "MISSING_MAX";

  public static final String SOURCES = "SOURCES";
  public static final String DIGESTS = "DIGESTS";

  /** Existing params derived from push down action, e.g., SORT_EXPR push down */
  Map<String, Object> existingParams;

  List<RelDataTypeField> inputFieldList;
  Map<String, ExprType> fieldTypes;

  /**
   * Records the source of each parameter, it decides which kind of source to retrieve value.
   *
   * <p>0 stands for DOC_VALUE
   *
   * <p>1 stand for SOURCE
   *
   * <p>2 stands for LITERAL
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
      List<RelDataTypeField> inputFieldList, Map<String, ExprType> fieldTypes) {
    this(inputFieldList, fieldTypes, Collections.emptyMap());
  }

  public ScriptParameterHelper(
      List<RelDataTypeField> inputFieldList,
      Map<String, ExprType> fieldTypes,
      Map<String, Object> params) {
    this.existingParams = params;
    this.inputFieldList = inputFieldList;
    this.fieldTypes = fieldTypes;
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
}

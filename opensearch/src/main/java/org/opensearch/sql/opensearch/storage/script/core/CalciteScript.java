/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.core;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.ScriptDataContext;

/**
 * Calcite script executor that executes the generated code on each document and determine if the
 * document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
public class CalciteScript {

  /** Function to execute. */
  private final Function1<DataContext, Object[]> function;

  private final Map<String, Object> params;

  /** Expression constructor. */
  public CalciteScript(Function1<DataContext, Object[]> function, Map<String, Object> params) {
    this.function = function;
    this.params = params;
  }

  /**
   * Evaluate on the doc generate by the doc provider.
   *
   * @param docProvider doc look up
   * @param sourceLookup source look up
   * @return expr value
   */
  public Object[] execute(
      Map<String, ScriptDocValues<?>> docProvider,
      SourceLookup sourceLookup,
      Map<String, Integer> parametersToIndex) {
    return AccessController.doPrivileged(
        (PrivilegedAction<Object[]>)
            () ->
                function.apply(
                    new ScriptDataContext(docProvider, sourceLookup, params, parametersToIndex)));
  }
}

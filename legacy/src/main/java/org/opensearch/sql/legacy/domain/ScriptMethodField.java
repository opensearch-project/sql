/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import java.util.List;

/** Stores information about function name for script fields */
public class ScriptMethodField extends MethodField {
  private final String functionName;

  public ScriptMethodField(
      String functionName, List<KVValue> params, SQLAggregateOption option, String alias) {
    super("script", params, option, alias);
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public boolean isScriptField() {
    return true;
  }
}

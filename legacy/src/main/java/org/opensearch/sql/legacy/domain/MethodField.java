/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.parser.NestedType;
import org.opensearch.sql.legacy.utils.Util;

/**
 * @author ansj
 */
public class MethodField extends Field {
  private List<KVValue> params = null;

  public MethodField(String name, List<KVValue> params, SQLAggregateOption option, String alias) {
    super(name, alias);
    this.params = params;
    this.option = option;
    if (alias == null || alias.trim().length() == 0) {
      Map<String, Object> paramsAsMap = this.getParamsAsMap();
      if (paramsAsMap.containsKey("alias")) {
        this.setAlias(paramsAsMap.get("alias").toString());
      } else {
        this.setAlias(this.toString());
      }
    }
  }

  public List<KVValue> getParams() {
    return params;
  }

  public Map<String, Object> getParamsAsMap() {
    Map<String, Object> paramsAsMap = new HashMap<>();
    if (this.params == null) {
      return paramsAsMap;
    }
    for (KVValue kvValue : this.params) {
      paramsAsMap.put(kvValue.key, kvValue.value);
    }
    return paramsAsMap;
  }

  @Override
  public String toString() {
    if (option != null) {
      return this.name + "(" + option + " " + Util.joiner(params, ",") + ")";
    }
    return this.name + "(" + Util.joiner(params, ",") + ")";
  }

  @Override
  public boolean isNested() {
    Map<String, Object> paramsAsMap = this.getParamsAsMap();
    return paramsAsMap.containsKey("nested") || paramsAsMap.containsKey("reverse_nested");
  }

  @Override
  public boolean isReverseNested() {
    return this.getParamsAsMap().containsKey("reverse_nested");
  }

  @Override
  public String getNestedPath() {
    if (!this.isNested()) {
      return null;
    }
    if (this.isReverseNested()) {
      String reverseNestedPath = this.getParamsAsMap().get("reverse_nested").toString();
      return reverseNestedPath.isEmpty() ? null : reverseNestedPath;
    }

    // Fix bug: NestedType.toString() isn't implemented which won't return desired nested path
    Object nestedField = getParamsAsMap().get("nested");
    if (nestedField instanceof NestedType) {
      return ((NestedType) nestedField).path;
    }
    return nestedField.toString();
  }

  @Override
  public boolean isChildren() {
    Map<String, Object> paramsAsMap = this.getParamsAsMap();
    return paramsAsMap.containsKey("children");
  }

  @Override
  public String getChildType() {
    if (!this.isChildren()) {
      return null;
    }

    return this.getParamsAsMap().get("children").toString();
  }

  @Override
  public boolean isScriptField() {
    return "script".equals(getName());
  }
}

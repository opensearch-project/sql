/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.core;

import java.util.List;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.sql.api.spec.LanguageSpec;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;

/**
 * Core extension that extends the default language spec with additional functions and capabilities.
 */
public class CoreExtension implements LanguageSpec.LanguageExtension {

  @Override
  public SqlOperatorTable operators() {
    return UnifiedFunctionSpec.SCALAR.operatorTable();
  }

  @Override
  public List<RelShuttle> preCompilationRules() {
    return List.of(new LateBindingFunctionRule());
  }
}

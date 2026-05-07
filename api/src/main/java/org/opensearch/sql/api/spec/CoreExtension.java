/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import org.apache.calcite.sql.SqlOperatorTable;

/**
 * Core language extension: contributes library functions (LENGTH, REGEXP_REPLACE, DATE_TRUNC) that
 * extend the standard Calcite operator table. These functions are shared across SQL and PPL.
 */
public class CoreExtension implements LanguageSpec.LanguageExtension {

  @Override
  public SqlOperatorTable operators() {
    return UnifiedFunctionSpec.LIBRARY.operatorTable();
  }
}

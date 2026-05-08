/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;

/** Search Extension: relevance functions and named argument rewriting. */
public class SearchExtension implements LanguageSpec.LanguageExtension {

  @Override
  public SqlOperatorTable operators() {
    return UnifiedFunctionSpec.RELEVANCE.operatorTable();
  }

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(NamedArgRewriter.INSTANCE);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.lang;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.QueryType;

/**
 * PPL language specification implementation.
 *
 * <p>This class provides a singleton implementation of {@link LangSpec} for PPL. It defines a
 * custom mapping from expression types to PPL type names.
 */
public class PPLLangSpec implements LangSpec {

  public static final PPLLangSpec PPL_SPEC = new PPLLangSpec();

  private static Map<ExprType, String> exprTypeToPPLType = new HashMap<>();

  static {
    exprTypeToPPLType.put(ExprCoreType.BYTE, "tinyint");
    exprTypeToPPLType.put(ExprCoreType.SHORT, "smallint");
    exprTypeToPPLType.put(ExprCoreType.INTEGER, "int");
    exprTypeToPPLType.put(ExprCoreType.LONG, "bigint");
  }

  private PPLLangSpec() {}

  @Override
  public QueryType language() {
    return QueryType.PPL;
  }

  /**
   * Returns the corresponding PPL type name for the given expression type. If the expression type
   * is not mapped, it returns the default type name.
   *
   * @param exprType the expression type.
   * @return the PPL type name associated with the expression type, or the default type name.
   */
  @Override
  public String typeName(ExprType exprType) {
    return exprTypeToPPLType.getOrDefault(exprType, exprType.typeName());
  }
}

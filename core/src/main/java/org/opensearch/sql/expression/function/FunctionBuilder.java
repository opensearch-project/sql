/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import java.util.List;
import org.opensearch.sql.expression.Expression;

/**
 * The definition of function which create {@link FunctionImplementation}
 * from input {@link Expression} list.
 */
public interface FunctionBuilder {

  /**
   * Create {@link FunctionImplementation} from input {@link Expression} list.
   *
   * @param arguments {@link Expression} list
   * @return {@link FunctionImplementation}
   */
  FunctionImplementation apply(QueryContext qc, List<Expression> arguments);
}

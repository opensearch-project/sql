/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.opensearch.sql.expression.Expression;

/** The definition of Function Implementation. */
public interface FunctionImplementation {

  /** Get Function Name. */
  FunctionName getFunctionName();

  /** Get Function Arguments. */
  List<Expression> getArguments();
}

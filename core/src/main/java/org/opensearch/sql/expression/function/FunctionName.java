/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** The definition of Function Name. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class FunctionName implements Serializable {
  @Getter private final String functionName;

  public static FunctionName of(String functionName) {
    return new FunctionName(functionName.toLowerCase(Locale.ROOT));
  }

  @Override
  public String toString() {
    return functionName;
  }
}

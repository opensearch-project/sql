/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.time.Clock;

public interface FunctionProperties extends Serializable {
  Clock getSystemClock();

  Clock getQueryStartClock();

  /**
   * Use when compiling functions that do not rely on function properties.
   */
  FunctionProperties None = new FunctionProperties() {
    @Override
    public Clock getSystemClock() {
      throw new UnexpectedCallException();
    }

    @Override
    public Clock getQueryStartClock() {
      throw new UnexpectedCallException();
    }
  };

  class UnexpectedCallException extends RuntimeException {
    public UnexpectedCallException() {
      super("FunctionProperties.None is a null object and not meant to be accessed.");
    }
  }
}

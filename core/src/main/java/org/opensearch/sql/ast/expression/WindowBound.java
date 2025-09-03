/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.Getter;

public abstract class WindowBound {
  private WindowBound() {}

  @Getter
  public static class OffSetWindowBound extends WindowBound {
    private final long offset;
    private final boolean isPreceding;

    OffSetWindowBound(long offset, boolean isPreceding) {
      this.offset = offset;
      this.isPreceding = isPreceding;
    }

    public boolean isPreceding() {
      return isPreceding;
    }
  }

  public static class CurrentRowWindowBound extends WindowBound {
    CurrentRowWindowBound() {}

    @Override
    public String toString() {
      return "CURRENT ROW";
    }
  }

  public static class UnboundedWindowBound extends WindowBound {
    private final boolean isPreceding;

    UnboundedWindowBound(boolean isPreceding) {
      this.isPreceding = isPreceding;
    }

    public boolean isPreceding() {
      return isPreceding;
    }

    @Override
    public boolean equals(Object o) {
      return this == o
          || o instanceof UnboundedWindowBound
              && isPreceding == ((UnboundedWindowBound) o).isPreceding;
    }

    @Override
    public String toString() {
      return isPreceding ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
    }
  }
}

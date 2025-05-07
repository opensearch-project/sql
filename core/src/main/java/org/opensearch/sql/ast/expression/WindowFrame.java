/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class WindowFrame extends UnresolvedExpression {
  private final FrameType type;
  private final WindowBound lower;
  private final WindowBound upper;

  public enum FrameType {
    RANGE,
    ROWS
  }

  public static WindowFrame defaultFrame() {
    return new WindowFrame(
        FrameType.ROWS, createBound("UNBOUNDED PRECEDING"), createBound("UNBOUNDED FOLLOWING"));
  }

  public static WindowFrame create(FrameType type, Literal lower, Literal upper) {
    WindowBound lowerBound = null;
    WindowBound upperBound = null;
    if (lower != null) {
      if (lower.getType() == DataType.STRING) {
        lowerBound = createBound(lower.getValue().toString());
      } else {
        throw new IllegalArgumentException(
            String.format("Unsupported bound type: %s", lower.getType()));
      }
    }
    if (upper != null) {
      if (upper.getType() == DataType.STRING) {
        upperBound = createBound(upper.getValue().toString());
      } else {
        throw new IllegalArgumentException(
            String.format("Unsupported bound type: %s", upper.getType()));
      }
    }
    return new WindowFrame(type, lowerBound, upperBound);
  }

  private static WindowBound createBound(String boundType) {
    boundType = boundType.trim().toUpperCase(Locale.ROOT);
    if ("CURRENT ROW".equals(boundType)) {
      return new WindowBound.CurrentRowWindowBound();
    } else if ("UNBOUNDED PRECEDING".equals(boundType)) {
      return new WindowBound.UnboundedWindowBound(true);
    } else if ("UNBOUNDED FOLLOWING".equals(boundType)) {
      return new WindowBound.UnboundedWindowBound(false);
    } else if (boundType.endsWith(" PRECEDING")) {
      long number = Long.parseLong(boundType.split(" PRECEDING")[0]);
      return new WindowBound.OffSetWindowBound(number, true);
    } else if (boundType.endsWith(" FOLLOWING")) {
      long number = Long.parseLong(boundType.split(" FOLLOWING")[0]);
      return new WindowBound.OffSetWindowBound(number, false);
    } else {
      throw new IllegalArgumentException(String.format("Unsupported bound type: %s", boundType));
    }
  }
}

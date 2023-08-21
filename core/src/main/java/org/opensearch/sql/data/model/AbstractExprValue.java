/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.exception.ExpressionEvaluationException;

/** Abstract ExprValue. */
public abstract class AbstractExprValue implements ExprValue {
  /** The customize compareTo logic. */
  @Override
  public int compareTo(ExprValue other) {
    if (this.isNull() || this.isMissing() || other.isNull() || other.isMissing()) {
      throw new IllegalStateException(
          "[BUG] Unreachable, Comparing with NULL or MISSING is undefined");
    }
    if ((this.isNumber() && other.isNumber())
        || (this.isDateTime() && other.isDateTime())
        || this.type().equals(other.type())) {
      return compare(other);
    } else {
      throw new ExpressionEvaluationException(
          String.format(
              "compare expected value have same type, but with [%s, %s]",
              this.type(), other.type()));
    }
  }

  /**
   * The customize equals logic.<br>
   * The table below list the NULL and MISSING handling logic.
   *
   * <table>
   *   <tr>
   *     <th>A</th>
   *     <th>B</th>
   *     <th>A == B</th>
   *   </tr>
   *   <tr>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>NULL</td>
   *     <td>MISSING</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>MISSING</td>
   *     <td>NULL</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>MISSING</td>
   *     <td>MISSING</td>
   *     <td>TRUE</td>
   *   </tr>
   * </table>
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (!(o instanceof ExprValue)) {
      return false;
    }
    ExprValue other = (ExprValue) o;
    if (this.isNull() || this.isMissing()) {
      return equal(other);
    } else if (other.isNull() || other.isMissing()) {
      return other.equals(this);
    } else {
      return equal(other);
    }
  }

  /** The expression value compare. */
  public abstract int compare(ExprValue other);

  /** The expression value equal. */
  public abstract boolean equal(ExprValue other);
}

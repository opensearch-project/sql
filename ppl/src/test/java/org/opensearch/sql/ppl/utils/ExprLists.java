/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.List;
import org.opensearch.sql.ast.expression.Argument;

public class ExprLists {
  /**
   * Apply all the given argument updates to the base argument list
   *
   * @param base The original list of arguments
   * @param updates All updates to apply on the base (either adding new results or updating existing
   *     ones)
   * @return The merged result
   */
  public static List<Argument> merge(List<Argument> base, Argument... updates) {
    for (Argument update : updates) {
      boolean updated = false;
      for (int i = 0; i < base.size(); i++) {
        if (base.get(i).getArgName().equals(update.getArgName())) {
          base.set(i, update);
          updated = true;
        }
      }
      if (!updated) {
        base.add(update);
      }
    }
    return base;
  }
}

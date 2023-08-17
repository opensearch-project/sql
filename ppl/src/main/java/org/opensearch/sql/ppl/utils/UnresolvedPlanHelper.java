/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableList;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** The helper to add select to {@link UnresolvedPlan} if needed. */
@UtilityClass
public class UnresolvedPlanHelper {

  /** Attach Select All to PPL commands if required. */
  public UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression.subquery;

import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** A runtime search scalar subquery that is planned independently from its parent query. */
public class RuntimeSearchScalarSubquery extends ScalarSubquery {

  public RuntimeSearchScalarSubquery(UnresolvedPlan query) {
    super(query);
  }
}

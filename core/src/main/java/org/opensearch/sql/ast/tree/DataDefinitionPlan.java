/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Data definition AST node.
 */
@Getter
@RequiredArgsConstructor
@ToString
public class DataDefinitionPlan extends UnresolvedPlan {

  private final DataDefinitionTask task;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }
}

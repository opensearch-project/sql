/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Extend Relation to describe the table itself */
@ToString
@EqualsAndHashCode(callSuper = false)
public class DescribeRelation extends Relation {
  public DescribeRelation(UnresolvedExpression tableName) {
    super(Collections.singletonList(tableName));
  }
}

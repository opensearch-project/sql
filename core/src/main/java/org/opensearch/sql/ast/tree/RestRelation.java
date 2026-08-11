/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Extend Relation to mark a {@code rest} leading command. The single table name is a reserved,
 * encoded token (produced by {@link org.opensearch.sql.utils.SystemIndexUtils#restTable}) that
 * carries the validated REST endpoint spec; it resolves through the storage engine to a REST source
 * table on the Calcite path, exactly as {@link DescribeRelation} resolves to a system index.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class RestRelation extends Relation {
  public RestRelation(UnresolvedExpression tableName) {
    super(Collections.singletonList(tableName));
  }
}

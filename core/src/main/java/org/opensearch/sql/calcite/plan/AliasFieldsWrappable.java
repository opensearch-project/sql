/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * Wrapper for TableScan to add alias fields by creating another project with alias upon on it. This
 * allows TableScan or Table to emit alias type fields in its schema, while it still supports
 * resolving these fields used in the query.
 */
public interface AliasFieldsWrappable {

  /**
 * Provide the alias-to-original field name mapping for this rel node.
 *
 * The map's keys are alias field names exposed in the schema and the values are
 * the corresponding original field names in the underlying relational expression.
 *
 * @return a map where each key is an alias field name and each value is the original field name it aliases
 */
Map<String, String> getAliasMapping();

  /**
   * Wraps the current relational node with a projection that adds alias fields defined by {@link #getAliasMapping()}.
   *
   * <p>The method creates aliased expressions for each mapping (alias -> original field) and applies them
   * on top of the existing node using {@code projectPlus}.
   *
   * @param relBuilder RelBuilder whose top node must implement {@code AliasFieldsWrappable}; the projection is applied to that node
   * @return the resulting {@link RelNode} after adding the alias-field projection
   */
  default RelNode wrapProjectForAliasFields(RelBuilder relBuilder) {
    assert relBuilder.peek() instanceof AliasFieldsWrappable
        : "The top node in RelBuilder must be AliasFieldsWrappable";
    Set<Entry<String, String>> aliasFieldsSet = this.getAliasMapping().entrySet();
    // Adding alias referring to the original field.
    List<RexNode> aliasFieldsNew =
        aliasFieldsSet.stream()
            .map(entry -> relBuilder.alias(relBuilder.field(entry.getValue()), entry.getKey()))
            .toList();
    return relBuilder.projectPlus(aliasFieldsNew).peek();
  }
}
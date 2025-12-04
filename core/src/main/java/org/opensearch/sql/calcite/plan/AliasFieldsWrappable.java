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

  Map<String, String> getAliasMapping();

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

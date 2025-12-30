/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

public interface OpenSearchRuleConfig extends RelRule.Config {

  /** Return a custom RelBuilderFactory for creating OpenSearchRelBuilder */
  @Override
  @Value.Default
  default RelBuilderFactory relBuilderFactory() {
    return CalciteToolsHelper.proto(Contexts.of(RelFactories.DEFAULT_STRUCT));
  }
}

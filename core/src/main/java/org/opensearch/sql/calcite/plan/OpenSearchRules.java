/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.convert.ConverterRule;

public class OpenSearchRules {
  public static final List<ConverterRule> OPEN_SEARCH_OPT_RULES = ImmutableList.of();

  // prevent instantiation
  private OpenSearchRules() {}
}

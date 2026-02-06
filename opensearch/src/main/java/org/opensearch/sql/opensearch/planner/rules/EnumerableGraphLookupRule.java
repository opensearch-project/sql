/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableGraphLookup;

/** Rule to convert a {@link LogicalGraphLookup} to a {@link CalciteEnumerableGraphLookup}. */
public class EnumerableGraphLookupRule extends ConverterRule {

  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              LogicalGraphLookup.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableGraphLookupRule")
          .withRuleFactory(EnumerableGraphLookupRule::new);

  /** Creates an EnumerableGraphLookupRule. */
  protected EnumerableGraphLookupRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalGraphLookup graphLookup = call.rel(0);
    // Only match if we can extract the OpenSearchIndex from the lookup table
    return extractOpenSearchIndex(graphLookup.getLookup()) != null;
  }

  /**
   * Recursively extracts OpenSearchIndex from a RelNode by traversing down to find the index scan.
   *
   * @param node The RelNode to extract from
   * @return The OpenSearchIndex, or null if not found
   */
  private static OpenSearchIndex extractOpenSearchIndex(RelNode node) {
    if (node instanceof AbstractCalciteIndexScan scan) {
      return scan.getOsIndex();
    }
    if (node instanceof RelSubset subset) {
      return extractOpenSearchIndex(subset.getOriginal());
    }
    // Recursively check inputs
    for (RelNode input : node.getInputs()) {
      OpenSearchIndex index = extractOpenSearchIndex(input);
      if (index != null) {
        return index;
      }
    }
    return null;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalGraphLookup graphLookup = (LogicalGraphLookup) rel;

    // Extract the OpenSearchIndex from the lookup table
    OpenSearchIndex lookupIndex = extractOpenSearchIndex(graphLookup.getLookup());
    if (lookupIndex == null) {
      throw new IllegalStateException("Cannot extract OpenSearchIndex from lookup table");
    }

    // Convert inputs to enumerable convention
    RelTraitSet traitSet = graphLookup.getTraitSet().replace(EnumerableConvention.INSTANCE);

    RelNode convertedSource =
        convert(
            graphLookup.getSource(),
            graphLookup.getSource().getTraitSet().replace(EnumerableConvention.INSTANCE));
    RelNode convertedLookup =
        convert(
            graphLookup.getLookup(),
            graphLookup.getLookup().getTraitSet().replace(EnumerableConvention.INSTANCE));
    return new CalciteEnumerableGraphLookup(
        graphLookup.getCluster(),
        traitSet,
        convertedSource,
        convertedLookup,
        graphLookup.getStartField(),
        graphLookup.getFromField(),
        graphLookup.getToField(),
        graphLookup.getOutputField(),
        graphLookup.getDepthField(),
        graphLookup.getMaxDepth(),
        graphLookup.isBidirectional(),
        graphLookup.isSupportArray(),
        graphLookup.isBatchMode());
  }
}

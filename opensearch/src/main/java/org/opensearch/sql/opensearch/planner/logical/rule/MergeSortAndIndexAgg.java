/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexAgg;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Merge Sort -- IndexScanAggregation to IndexScanAggregation.
 */
public class MergeSortAndIndexAgg implements Rule<LogicalSort> {

  private final Capture<OpenSearchLogicalIndexAgg> indexAggCapture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeAggAndIndexScan.
   */
  public MergeSortAndIndexAgg() {
    this.indexAggCapture = Capture.newCapture();
    final AtomicReference<LogicalSort> sortRef = new AtomicReference<>();

    this.pattern = typeOf(LogicalSort.class)
        .matching(OptimizationRuleUtils::sortByFieldsOnly)
        .matching(OptimizationRuleUtils::sortByDefaultOptionOnly)
        .matching(sort -> {
          sortRef.set(sort);
          return true;
        })
        .with(source().matching(typeOf(OpenSearchLogicalIndexAgg.class)
            .matching(indexAgg -> !hasAggregatorInSortBy(sortRef.get(), indexAgg))
            .capturedAs(indexAggCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    OpenSearchLogicalIndexAgg indexAgg = captures.get(indexAggCapture);
    return OpenSearchLogicalIndexAgg.builder()
        .relationName(indexAgg.getRelationName())
        .filter(indexAgg.getFilter())
        .groupByList(indexAgg.getGroupByList())
        .aggregatorList(indexAgg.getAggregatorList())
        .sortList(sort.getSortList())
        .build();
  }

  private boolean hasAggregatorInSortBy(LogicalSort sort, OpenSearchLogicalIndexAgg agg) {
    final Set<String> aggregatorNames =
        agg.getAggregatorList().stream().map(NamedAggregator::getName).collect(Collectors.toSet());
    for (Pair<Sort.SortOption, Expression> sortPair : sort.getSortList()) {
      if (aggregatorNames.contains(((ReferenceExpression) sortPair.getRight()).getAttr())) {
        return true;
      }
    }
    return false;
  }
}

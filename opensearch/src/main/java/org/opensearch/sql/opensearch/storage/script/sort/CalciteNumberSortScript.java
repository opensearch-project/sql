/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.NumberSortScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite number sort script. */
@EqualsAndHashCode(callSuper = false)
public class CalciteNumberSortScript extends NumberSortScript {

  /** Calcite script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;
  private final LeafReaderContext context;
  private final Direction direction;
  private final NullDirection nullDirection;

  public CalciteNumberSortScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    // TODO: we'd better get source from the leafLookup of super once it's available
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.context = context;
    this.direction =
        params.containsKey(PlanUtils.DIRECTION)
            ? Direction.valueOf((String) params.get(PlanUtils.DIRECTION))
            : Direction.ASCENDING;
    this.nullDirection =
        params.containsKey(PlanUtils.NULL_DIRECTION)
            ? NullDirection.valueOf((String) params.get(PlanUtils.NULL_DIRECTION))
            : NullDirection.FIRST;
  }

  @Override
  public void setDocument(int docid) {
    super.setDocument(docid);
    this.sourceLookup.setSegmentAndDocument(context, docid);
  }

  @Override
  public double execute() {
    Object value = calciteScript.execute(this.getDoc(), this.sourceLookup)[0];
    // There is a limitation here when the Double value is exactly theoretical min/max value.
    // It can't distinguish the ordering between null and exact Double.NEGATIVE_INFINITY or
    // Double.NaN.
    if (value == null) {
      boolean isAscending = direction == Direction.ASCENDING;
      boolean isNullFirst = nullDirection == NullDirection.FIRST;
      return isAscending == isNullFirst ? Double.NEGATIVE_INFINITY : Double.NaN;
    }
    return ((Number) value).doubleValue();
  }
}

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
import org.opensearch.script.StringSortScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite string sort script. */
@EqualsAndHashCode(callSuper = false)
public class CalciteStringSortScript extends StringSortScript {

  /** Calcite script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;
  private final Direction direction;
  private final NullDirection nullDirection;

  private static final String MAX_SENTINEL = "\uFFFF\uFFFF_NULL_PLACEHOLDER_";
  private static final String MIN_SENTINEL = "\u0000\u0000_NULL_PLACEHOLDER_";

  public CalciteStringSortScript(
      Function1<DataContext, Object[]> function,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    // TODO: we'd better get source from the leafLookup of super once it's available
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
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
  public String execute() {
    Object value = calciteScript.execute(this.getDoc(), this.sourceLookup)[0];
    // There is a limitation here when the String value is larger or smaller than sentinel values.
    // It can't guarantee the lexigraphic ordering between null and special strings.
    if (value == null) {
      boolean isAscending = direction == Direction.ASCENDING;
      boolean isNullFirst = nullDirection == NullDirection.FIRST;
      return isAscending == isNullFirst ? MIN_SENTINEL : MAX_SENTINEL;
    }
    return value.toString();
  }
}

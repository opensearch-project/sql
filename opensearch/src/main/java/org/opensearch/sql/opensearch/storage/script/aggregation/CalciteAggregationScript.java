/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.LocalTime;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite script executor that executes the generated code on each document for aggregation. */
@EqualsAndHashCode(callSuper = false)
class CalciteAggregationScript extends AggregationScript {

  /** Calcite Script. */
  private final CalciteScript calciteScript;

  private final LeafReaderContext context;

  private final SourceLookup sourceLookup;

  private final RelDataType type;

  private final Map<String, Integer> parametersToIndex;

  public CalciteAggregationScript(
      Function1<DataContext, Object[]> function,
      RelDataType type,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params,
      Map<String, Integer> parametersToIndex) {
    super(params, lookup, context);
    this.context = context;
    this.calciteScript = new CalciteScript(function, params);
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.type = type;
    this.parametersToIndex = parametersToIndex;
  }

  @Override
  public void setDocument(int docid) {
    super.setDocument(docid);
    this.sourceLookup.setSegmentAndDocument(context, docid);
  }

  @Override
  public Object execute() {
    Object value =
        calciteScript.execute(this.getDoc(), this.sourceLookup, this.parametersToIndex)[0];
    ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
    // See logic in {@link ExpressionAggregationScript::execute}
    ExprCoreType coreType = (ExprCoreType) exprType;
    switch (coreType) {
      case TIME:
        // Can't get timestamp from `ExprTimeValue`
        return MILLIS.between(LocalTime.MIN, ExprValueUtils.fromObjectValue(value, TIME).timeValue());
      case DATE:
        return ExprValueUtils.fromObjectValue(value, DATE).timestampValue().toEpochMilli();
      case TIMESTAMP:
        return ExprValueUtils.fromObjectValue(value, TIMESTAMP)
            .timestampValue()
            .toEpochMilli();
      default:
        return value;
    }
  }
}

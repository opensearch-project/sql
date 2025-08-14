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
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.opensearch.storage.script.core.CalciteScript;

/** Calcite script executor that executes the generated code on each document for aggregation. */
@EqualsAndHashCode(callSuper = false)
class CalciteAggregationScript extends AggregationScript {

  /** Calcite Script. */
  private final CalciteScript calciteScript;

  private final SourceLookup sourceLookup;

  private final RelDataType type;

  public CalciteAggregationScript(
      Function1<DataContext, Object[]> function,
      RelDataType type,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.calciteScript = new CalciteScript(function, params);
    this.sourceLookup = lookup.getLeafSearchLookup(context).source();
    this.type = type;
  }

  @Override
  public Object execute() {
    Object value = calciteScript.execute(this.getDoc(), this.sourceLookup)[0];
    ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
    // See logic in {@link ExpressionAggregationScript::execute}
    return switch ((ExprCoreType) exprType) {
      case TIME ->
      // Can't get timestamp from `ExprTimeValue`
      MILLIS.between(LocalTime.MIN, ExprValueUtils.fromObjectValue(value, TIME).timeValue());
      case DATE -> ExprValueUtils.fromObjectValue(value, DATE).timestampValue().toEpochMilli();
      case TIMESTAMP -> {
        // Handle timestamp strings from bin operations using proper function chain
        // When bin operations produce timestamp strings, they need to be converted to epoch
        // milliseconds for OpenSearch aggregation (which expects primitive numeric values)
        if (value instanceof String) {
          String strValue = (String) value;
          // Use proper function chain: STRING -> TIMESTAMP() -> epoch milliseconds
          try {
            ExprValue timestampValue =
                DateTimeFunctions.exprTimestampFromString(new ExprStringValue(strValue));
            if (!timestampValue.equals(ExprNullValue.of())) {
              long epochMillis = timestampValue.timestampValue().toEpochMilli();
              yield epochMillis;
            }
          } catch (Exception e) {
            // Fall back to ExprValueUtils parsing
            try {
              long epochMillis =
                  ExprValueUtils.fromObjectValue(strValue, TIMESTAMP)
                      .timestampValue()
                      .toEpochMilli();
              yield epochMillis;
            } catch (Exception fallbackEx) {
              // Last resort: return the original value
              yield value;
            }
          }
        }
        // Default handling for non-string timestamp values
        long epochMillis =
            ExprValueUtils.fromObjectValue(value, TIMESTAMP).timestampValue().toEpochMilli();
        yield epochMillis;
      }
      default -> {
        // Handle timestamp strings from bin operations using proper function chain
        // When bin operations produce timestamp strings, convert to epoch milliseconds for
        // aggregation
        if (value instanceof String) {
          String strValue = (String) value;
          // Use proper function chain: STRING -> TIMESTAMP() -> epoch milliseconds
          try {
            ExprValue timestampValue =
                DateTimeFunctions.exprTimestampFromString(new ExprStringValue(strValue));
            if (!timestampValue.equals(ExprNullValue.of())) {
              yield timestampValue.timestampValue().toEpochMilli();
            }
          } catch (Exception e) {
            // Fall back to ExprValueUtils parsing
            try {
              yield ExprValueUtils.fromObjectValue(strValue, TIMESTAMP)
                  .timestampValue()
                  .toEpochMilli();
            } catch (Exception fallbackEx) {
              // Last resort: return the original value
              yield value;
            }
          }
        }
        yield value;
      }
    };
  }
}

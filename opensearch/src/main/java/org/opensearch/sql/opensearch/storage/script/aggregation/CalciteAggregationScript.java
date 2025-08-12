/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.LocalDateTime;
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
        // Handle timestamp strings from bin operations with FROM_UNIXTIME
        // When bin command uses FROM_UNIXTIME, it produces timestamp strings that need special
        // handling
        // This prevents "AggregationExecutionException[Unsupported script value [...], expected a
        // number, date, or boolean]"
        if (value instanceof String) {
          String strValue = (String) value;
          // Check if this is a timestamp string from FROM_UNIXTIME (format: YYYY-MM-DD HH:MM:SS)
          if (strValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?")) {
            // Parse manually using LocalDateTime - this is more reliable than ExprValueUtils
            try {
              LocalDateTime dateTime =
                  LocalDateTime.parse(
                      strValue,
                      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
              long epochMillis = dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
              yield epochMillis;
            } catch (Exception parseEx) {
              // If direct parsing fails, fall back to ExprValueUtils
              try {
                long epochMillis =
                    ExprValueUtils.fromObjectValue(strValue, TIMESTAMP)
                        .timestampValue()
                        .toEpochMilli();
                yield epochMillis;
              } catch (Exception e) {
                // Last resort: return the original value (this will likely cause aggregation error)
                yield value;
              }
            }
          }
        }
        // Default handling for non-string timestamp values
        long epochMillis =
            ExprValueUtils.fromObjectValue(value, TIMESTAMP).timestampValue().toEpochMilli();
        yield epochMillis;
      }
      default -> {
        // Handle timestamp strings from bin operations with FROM_UNIXTIME
        // When bin command uses FROM_UNIXTIME for grouping, it produces timestamp strings
        // that need to be converted to epoch milliseconds for OpenSearch aggregation
        if (value instanceof String) {
          String strValue = (String) value;
          // Check if this is a timestamp string from FROM_UNIXTIME (format: YYYY-MM-DD HH:MM:SS)
          if (strValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?")) {
            // Parse manually using LocalDateTime - this is more reliable than ExprValueUtils
            try {
              LocalDateTime dateTime =
                  LocalDateTime.parse(
                      strValue,
                      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
              yield dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (Exception parseEx) {
              // If direct parsing fails, fall back to ExprValueUtils
              try {
                yield ExprValueUtils.fromObjectValue(strValue, TIMESTAMP)
                    .timestampValue()
                    .toEpochMilli();
              } catch (Exception e) {
                // Last resort: return the original value
                yield value;
              }
            }
          }
        }
        yield value;
      }
    };
  }
}

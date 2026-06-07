/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.DateOnlyType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.schema.TimeOnlyType;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine adapter for the analytics engine (Project Mustang).
 *
 * <p>Bridges the analytics engine's {@link QueryPlanExecutor} with the SQL plugin's {@link
 * ExecutionEngine} response pipeline. Takes a Calcite {@link RelNode}, delegates execution to the
 * analytics engine, and converts the raw results into {@link QueryResponse}.
 */
public class AnalyticsExecutionEngine implements ExecutionEngine {

  // TIME-typed columns round-trip through Timestamp and arrive in list elements as
  // "1970-01-01[ T]HH:mm:ss[.fraction]"; analytics-engine post-processes scalars but
  // list-aggregation elements bypass that path (see list_merge in DataFusion).
  private static final Pattern EPOCH_DATE_TIME_PREFIX =
      Pattern.compile("^1970-01-01[ T](\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)$");

  // DATE-typed columns whose wire is Timestamp(ms) arrive as "YYYY-MM-DD HH:mm:ss";
  // when the column carries a DateOnlyType marker we strip the time suffix.
  private static final Pattern DATE_WITH_MIDNIGHT_TIME =
      Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})[ T]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?$");

  private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

  public AnalyticsExecutionEngine(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
    this.planExecutor = planExecutor;
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    execute(plan, context, null, listener);
  }

  /**
   * Overload that threads a {@link org.opensearch.analytics.QueryRequestContext} snapshot down to
   * the analytics-engine's plan executor via {@link
   * org.opensearch.analytics.exec.QueryPlanExecutor}'s opaque context slot. The snapshot is
   * captured once at query entry so planner and executor see the same cluster state.
   */
  public void execute(
      RelNode plan,
      CalcitePlanContext context,
      org.opensearch.analytics.QueryRequestContext queryCtx,
      ResponseListener<QueryResponse> listener) {
    // QueryPlanExecutor became asynchronous in analytics-framework 3.7 — execution is dispatched
    // to a worker pool and results arrive on the listener. Record the execute metric in the
    // listener callback, before delegating to the user-supplied listener, so the metric snapshot
    // taken by SimpleJsonResponseFormatter sees the correct value.
    ProfileContext profileCtx = QueryProfiling.current();
    long execStart = System.nanoTime();

    planExecutor.execute(
        plan,
        queryCtx,
        new ActionListener<>() {
          @Override
          public void onResponse(Iterable<Object[]> rows) {
            QueryProfiling.withCurrentContext(
                profileCtx,
                () -> {
                  try {
                    List<RelDataTypeField> fields = plan.getRowType().getFieldList();
                    List<ExprValue> results = convertRows(rows, fields);
                    Schema schema = buildSchema(fields);
                    profileCtx
                        .getOrCreateMetric(MetricName.EXECUTE)
                        .set(System.nanoTime() - execStart);
                    listener.onResponse(new QueryResponse(schema, results, Cursor.None));
                  } catch (Exception e) {
                    listener.onFailure(e);
                  }
                  return null;
                });
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    try {
      String logical = RelOptUtil.toString(plan, mode.toExplainLevel());
      ExplainResponse response =
          new ExplainResponse(new ExplainResponseNodeV2(logical, null, null));
      listener.onResponse(ExplainResponse.normalizeLf(response));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Executes the query with profiling enabled. Returns results + stage timing profile. Called when
   * {@code profile=true} is set on the request.
   */
  public void executeWithProfile(
      RelNode plan,
      CalcitePlanContext context,
      org.opensearch.analytics.QueryRequestContext queryCtx,
      ResponseListener<QueryResponse> listener) {

    planExecutor.executeWithProfile(
        plan,
        queryCtx,
        new ActionListener<>() {
          @Override
          public void onResponse(ProfiledResult result) {
            try {
              // ProfiledResult delivers the profile on BOTH success and failure paths
              // so users get stage timing visibility even when a query partially fails.
              QueryResponse response = buildProfiledResponse(plan, result);
              listener.onResponse(response);
            } catch (Exception e) {
              listener.onFailure(e);
            }
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }

  private QueryResponse buildProfiledResponse(RelNode plan, ProfiledResult result) {
    List<RelDataTypeField> fields = plan.getRowType().getFieldList();
    Schema schema = buildSchema(fields);
    List<ExprValue> results =
        result.rows() != null ? convertRows(result.rows(), fields) : List.of();
    QueryResponse response = new QueryResponse(schema, results, Cursor.None);
    response.setProfile(result.profile());
    if (!result.isSuccess()) {
      response.setError(result.failure());
    }
    return response;
  }

  private List<ExprValue> convertRows(Iterable<Object[]> rows, List<RelDataTypeField> fields) {
    List<ExprValue> results = new ArrayList<>();
    for (Object[] row : rows) {
      Map<String, ExprValue> valueMap = new LinkedHashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        RelDataTypeField field = fields.get(i);
        Object value = (i < row.length) ? row[i] : null;
        valueMap.put(field.getName(), toExprValue(value, field.getType()));
      }
      results.add(ExprTupleValue.fromExprValueMap(valueMap));
    }
    return results;
  }

  /**
   * Converts a single result cell to an {@link ExprValue}, dispatching on the column's UDT when
   * present so {@code byte[]} payloads are rendered correctly:
   *
   * <ul>
   *   <li>{@link IpType} + {@code byte[]} &rarr; canonical address string (matches {@code
   *       IpFieldMapper}'s {@code valueFetcher} output).
   *   <li>{@link BinaryType} + {@code byte[]} &rarr; base64-encoded string (matches the OpenSearch
   *       {@code binary} field wire format).
   *   <li>Anything else &rarr; existing {@link ExprValueUtils#fromObjectValue} path.
   * </ul>
   *
   * <p>Without this dispatch, {@code fromObjectValue} throws {@code unsupported object class [B} on
   * byte[] cells, and IP buffers leak through as raw 16-byte ipv4-mapped-ipv6 garbage.
   */
  private static ExprValue toExprValue(Object value, RelDataType type) {
    if (value instanceof byte[] bytes) {
      if (type instanceof IpType) {
        try {
          return ExprValueUtils.stringValue(
              InetAddresses.toAddrString(InetAddress.getByAddress(bytes)));
        } catch (UnknownHostException e) {
          throw new IllegalStateException("invalid IP buffer length: " + bytes.length, e);
        }
      } else if (type instanceof BinaryType) {
        return ExprValueUtils.stringValue(Base64.getEncoder().encodeToString(bytes));
      }
    }
    // span(date-typed) returns Timestamp(ms) wire with midnight time; render as YYYY-MM-DD only.
    if (type instanceof DateOnlyType && value instanceof String s) {
      var m = DATE_WITH_MIDNIGHT_TIME.matcher(s);
      if (m.matches()) {
        return ExprValueUtils.stringValue(m.group(1));
      }
    }
    // span(time-typed) returns Timestamp(ms) wire with 1970-01-01 prefix; render as HH:mm:ss only.
    if (type instanceof TimeOnlyType && value instanceof String s) {
      var m = EPOCH_DATE_TIME_PREFIX.matcher(s);
      if (m.matches()) {
        return ExprValueUtils.stringValue(m.group(1));
      }
    }
    // List elements that look like a sentinel-epoch-prefixed time render as HH:mm:ss only.
    if (value instanceof List<?> list) {
      return ExprValueUtils.collectionValue(stripEpochDatePrefixInList(list));
    }
    return ExprValueUtils.fromObjectValue(value);
  }

  /**
   * Returns a copy of {@code list} with each "1970-01-01[ T]HH:mm:ss[.fraction]" string replaced by
   * the time portion only; non-matching elements pass through unchanged.
   */
  private static List<Object> stripEpochDatePrefixInList(List<?> list) {
    List<Object> out = new ArrayList<>(list.size());
    for (Object element : list) {
      if (element instanceof String s) {
        var m = EPOCH_DATE_TIME_PREFIX.matcher(s);
        out.add(m.matches() ? m.group(1) : s);
      } else {
        out.add(element);
      }
    }
    return out;
  }

  private Schema buildSchema(List<RelDataTypeField> fields) {
    List<Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : fields) {
      ExprType exprType = convertType(field.getType());
      columns.add(new Schema.Column(field.getName(), null, exprType));
    }
    return new Schema(columns);
  }

  private ExprType convertType(RelDataType type) {
    try {
      return OpenSearchTypeFactory.convertAnalyticsEngineRelDataTypeToExprType(type);
    } catch (IllegalArgumentException e) {
      return org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
    }
  }
}

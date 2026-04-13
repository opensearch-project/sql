/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.google.common.base.Suppliers;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.TimewrapUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer) {
    this.client = client;
    this.executionProtector = executionProtector;
    this.planSerializer = planSerializer;
    registerOpenSearchFunctions();
  }

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    execute(physicalPlan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan physicalPlan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();

            context.getSplit().ifPresent(plan::add);
            plan.open();

            Integer querySizeLimit = context.getQuerySizeLimit();
            while (plan.hasNext() && (querySizeLimit == null || result.size() < querySizeLimit)) {
              result.add(plan.next());
            }

            QueryResponse response =
                new QueryResponse(
                    physicalPlan.schema(), result, planSerializer.convertToCursor(plan));
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            Explain openSearchExplain =
                new Explain() {
                  @Override
                  public ExplainResponseNode visitTableScan(
                      TableScanOperator node, Object context) {
                    return explain(
                        node,
                        context,
                        explainNode -> {
                          explainNode.setDescription(Map.of("request", node.explain()));
                        });
                  }
                };

            listener.onResponse(openSearchExplain.apply(plan));
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  private Hook.Closeable getPhysicalPlanInHook(
      AtomicReference<String> physical, SqlExplainLevel level) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        obj -> {
          RelRoot relRoot = (RelRoot) obj;
          physical.set(RelOptUtil.toString(relRoot.rel, level));
        });
  }

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (mode == ExplainMode.SIMPLE) {
              String logical = RelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES);
              listener.onResponse(
                  new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
            } else {
              SqlExplainLevel level =
                  mode == ExplainMode.COST
                      ? SqlExplainLevel.ALL_ATTRIBUTES
                      : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
              String logical = RelOptUtil.toString(rel, level);
              AtomicReference<String> physical = new AtomicReference<>();
              AtomicReference<String> javaCode = new AtomicReference<>();
              try (Hook.Closeable closeable = getPhysicalPlanInHook(physical, level)) {
                if (mode == ExplainMode.EXTENDED) {
                  getCodegenInHook(javaCode);
                  CalcitePlanContext.skipEncoding.set(true);
                }
                // triggers the hook
                OpenSearchRelRunners.run(context, rel);
              }
              listener.onResponse(
                  new ExplainResponse(
                      new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
            }
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            CalcitePlanContext.skipEncoding.remove();
          }
        });
  }

  @Override
  public void execute(
      RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    client.schedule(
        () -> {
          try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
            ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
            long execTime = System.nanoTime();
            ResultSet result = statement.executeQuery();
            QueryResponse response =
                buildResultSet(result, rel.getRowType(), context.sysLimit.querySizeLimit());
            metric.add(System.nanoTime() - execTime);
            listener.onResponse(response);

          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Process values recursively, handling geo points, nested maps, structs and arrays. When a {@link
   * RelDataType} is provided, struct values (StructImpl) are converted to Maps keyed by field
   * names, preserving field-name information in the JSON output.
   *
   * @param value The raw value from the JDBC result set
   * @param type The Calcite type metadata for this value, or null if unavailable
   */
  @SuppressWarnings("unchecked")
  private static Object processValue(Object value, RelDataType type) throws SQLException {
    if (value == null) {
      return null;
    }
    if (value instanceof Point point) {
      return new OpenSearchExprGeoPointValue(point.getY(), point.getX());
    }
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      Map<String, Object> convertedMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        convertedMap.put(entry.getKey(), processValue(entry.getValue(), null));
      }
      return convertedMap;
    }
    if (value instanceof StructImpl structImpl) {
      Object[] attrs = structImpl.getAttributes();
      if (type != null && type.getSqlTypeName() == SqlTypeName.ROW) {
        List<RelDataTypeField> fields = type.getFieldList();
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.size() && i < attrs.length; i++) {
          map.put(fields.get(i).getName(), processValue(attrs[i], fields.get(i).getType()));
        }
        return map;
      }
      return Arrays.asList(attrs);
    }
    if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      RelDataType componentType =
          (type != null && type.getComponentType() != null) ? type.getComponentType() : null;
      List<Object> convertedList = new ArrayList<>();
      for (Object item : list) {
        convertedList.add(processValue(item, componentType));
      }
      return convertedList;
    }
    // For other types, return as-is
    return value;
  }

  private QueryResponse buildResultSet(
      ResultSet resultSet, RelDataType rowTypes, Integer querySizeLimit) throws SQLException {
    // Get the ResultSet metadata to know about columns
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();
    // Iterate through the ResultSet
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      Map<String, ExprValue> row = new LinkedHashMap<String, ExprValue>();
      // Loop through each column
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = resultSet.getObject(columnName);
        Object converted = processValue(value, fieldTypes.get(i - 1));
        ExprValue exprValue = ExprValueUtils.fromObjectValue(converted);
        row.put(columnName, exprValue);
      }
      values.add(ExprTupleValue.fromExprValueMap(row));
    }

    List<Column> columns = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= columnCount; ++i) {
      String columnName = metaData.getColumnName(i);
      RelDataType fieldType = fieldTypes.get(i - 1);
      // TODO: Correct this after fixing issue github.com/opensearch-project/sql/issues/3751
      //  The element type of struct and array is currently set to ANY.
      //  We set them using the runtime type as a workaround.
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        if (!values.isEmpty()) {
          exprType = values.getFirst().tupleValue().get(columnName).type();
        } else {
          // Using UNDEFINED instead of UNKNOWN to avoid throwing exception
          exprType = ExprCoreType.UNDEFINED;
        }
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
      }
      columns.add(new Column(columnName, null, exprType));
    }
    // Timewrap post-processing: pivot unpivoted rows into period columns
    // Input: [display_ts, value_col(s)..., __base_offset__, __period__]
    // Output: [display_ts, <value_prefix>_<period_name>, ...]
    if (CalcitePlanContext.stripNullColumns.get()) {
      try {
        String unitInfo = CalcitePlanContext.timewrapUnitName.get();
        if (unitInfo != null && !values.isEmpty()) {
          // Find column indices
          int tsIdx = 0;
          int periodIdx = -1;
          int baseOffsetIdx = -1;
          List<Integer> valueIdxs = new ArrayList<>();
          for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName();
            if ("__period__".equals(name)) periodIdx = i;
            else if ("__base_offset__".equals(name)) baseOffsetIdx = i;
            else if (i > 0 && periodIdx < 0 && baseOffsetIdx < 0) valueIdxs.add(i);
          }
          // Workaround: if indices weren't found before value columns, scan again
          if (periodIdx < 0 || baseOffsetIdx < 0) {
            valueIdxs.clear();
            for (int i = 0; i < columns.size(); i++) {
              String name = columns.get(i).getName();
              if ("__period__".equals(name)) periodIdx = i;
              else if ("__base_offset__".equals(name)) baseOffsetIdx = i;
              else if (i > 0) valueIdxs.add(i);
            }
          }

          // Read __base_offset__ (constant across all rows)
          long baseOffset = 0;
          ExprValue boVal = values.getFirst().tupleValue().get("__base_offset__");
          if (boVal != null && !boVal.isNull()) {
            baseOffset = boVal.longValue();
          }

          // Collect distinct periods (sorted descending = oldest first in output)
          List<String> colNames = columns.stream().map(Column::getName).toList();
          Set<Long> periodSet = new java.util.TreeSet<>(java.util.Collections.reverseOrder());
          for (ExprValue row : values) {
            ExprValue pv = row.tupleValue().get("__period__");
            if (pv != null && !pv.isNull()) {
              periodSet.add(pv.longValue());
            }
          }
          List<Long> periods = new ArrayList<>(periodSet);

          // Build value column names
          List<String> valueColNames = new ArrayList<>();
          for (int vi : valueIdxs) {
            valueColNames.add(columns.get(vi).getName());
          }

          // Build output column names: [ts, val1_period1, val1_period2, ..., val2_period1, ...]
          // Splunk order: for each period, all value columns (oldest period first)
          List<String> outColNames = new ArrayList<>();
          outColNames.add(columns.get(tsIdx).getName());
          List<ExprType> outColTypes = new ArrayList<>();
          outColTypes.add(columns.get(tsIdx).getExprType());

          for (long period : periods) {
            for (int vi = 0; vi < valueColNames.size(); vi++) {
              String prefix = valueColNames.get(vi);
              String periodName = renameTimewrapPeriod(period, baseOffset, unitInfo);
              outColNames.add(prefix + "_" + periodName);
              outColTypes.add(columns.get(valueIdxs.get(vi)).getExprType());
            }
          }

          // Group rows by display_ts, pivot periods into columns
          // Use LinkedHashMap to preserve insertion order (sorted by ts from Calcite)
          Map<String, Map<String, ExprValue>> pivoted = new LinkedHashMap<>();
          String tsColName = columns.get(tsIdx).getName();
          for (ExprValue row : values) {
            java.util.Map<String, ExprValue> tuple = row.tupleValue();
            String tsKey = tuple.get(tsColName).toString();
            long period = tuple.get("__period__").longValue();

            Map<String, ExprValue> outRow =
                pivoted.computeIfAbsent(
                    tsKey,
                    k -> {
                      Map<String, ExprValue> r = new LinkedHashMap<>();
                      r.put(outColNames.get(0), tuple.get(tsColName));
                      // Initialize all period columns to null
                      for (int i = 1; i < outColNames.size(); i++) {
                        r.put(outColNames.get(i), ExprNullValue.of());
                      }
                      return r;
                    });

            // Fill in the value for this period
            for (int vi = 0; vi < valueColNames.size(); vi++) {
              String prefix = valueColNames.get(vi);
              String periodName = renameTimewrapPeriod(period, baseOffset, unitInfo);
              String colName = prefix + "_" + periodName;
              ExprValue val = tuple.get(valueColNames.get(vi));
              if (val != null) {
                outRow.put(colName, val);
              }
            }
          }

          // Build output
          columns = new ArrayList<>();
          for (int i = 0; i < outColNames.size(); i++) {
            columns.add(new Column(outColNames.get(i), null, outColTypes.get(i)));
          }
          values = new ArrayList<>();
          for (Map<String, ExprValue> outRow : pivoted.values()) {
            values.add(ExprTupleValue.fromExprValueMap(outRow));
          }
        }
      } finally {
        CalcitePlanContext.stripNullColumns.set(false);
        CalcitePlanContext.timewrapUnitName.set(null);
        CalcitePlanContext.timewrapSeries.set(null);
        CalcitePlanContext.timewrapTimeFormat.set(null);
      }
    }

    Schema schema = new Schema(columns);
    QueryResponse response = new QueryResponse(schema, values, null);
    return response;
  }

  /**
   * Rename a timewrap period column from relative to absolute offset. Supports three series modes:
   * relative (default), short, and exact. unitInfo format: "spanValue|singular|plural|_before".
   */
  private String renameTimewrapColumn(String name, long baseOffset, String unitInfo) {
    String[] parts = unitInfo.split("\\|", -1);
    if (parts.length < 4) return name;
    int spanValue = Integer.parseInt(parts[0]);
    String singular = parts[1];
    String plural = parts[2];
    String nameSuffix = parts[3];

    if (!name.endsWith(nameSuffix)) return name;
    String beforeSuffix = name.substring(0, name.length() - nameSuffix.length());
    int lastUnderscore = beforeSuffix.lastIndexOf('_');
    if (lastUnderscore < 0) return name;

    String prefix = beforeSuffix.substring(0, lastUnderscore);
    String periodStr = beforeSuffix.substring(lastUnderscore + 1);
    try {
      int relativePeriod = Integer.parseInt(periodStr);
      long absolutePeriod = (baseOffset + relativePeriod - 1) * spanValue;

      String seriesMode = CalcitePlanContext.timewrapSeries.get();
      if (seriesMode == null) seriesMode = "relative";

      return switch (seriesMode) {
        case "short" ->
            // series=short: prefix_s<absolutePeriod>
            prefix + "_s" + absolutePeriod;
        case "exact" -> {
          // series=exact: prefix_<formatted_date>
          String timeFormat = CalcitePlanContext.timewrapTimeFormat.get();
          if (timeFormat == null) timeFormat = "%Y-%m-%d";
          // Compute period start timestamp: reference - absolutePeriod * spanSeconds
          // absolutePeriod is in span units, need to convert to seconds
          long spanSec =
              TimewrapUtils.spanToSeconds(
                      org.opensearch.sql.ast.expression.SpanUnit.of(singular.toUpperCase()), 1)
                  * spanValue;
          // Not enough info to compute exact date here — fall back to short naming
          // TODO: pass reference epoch + span for exact date computation
          yield prefix + "_s" + absolutePeriod;
        }
        default -> {
          // series=relative (default): prefix_<N><unit>_before/latest/after
          if (absolutePeriod == 0) {
            yield prefix + "_latest_" + singular;
          } else if (absolutePeriod > 0) {
            String unit = absolutePeriod == 1 ? singular : plural;
            yield prefix + "_" + absolutePeriod + unit + "_before";
          } else {
            long absPeriod = Math.abs(absolutePeriod);
            String unit = absPeriod == 1 ? singular : plural;
            yield prefix + "_" + absPeriod + unit + "_after";
          }
        }
      };
    } catch (NumberFormatException e) {
      return name;
    }
  }

  /**
   * Generate a period name from a relative period number and base offset. Returns the suffix only
   * (no value prefix). E.g., "2days_before", "latest_day", "s2".
   */
  private String renameTimewrapPeriod(long relativePeriod, long baseOffset, String unitInfo) {
    String[] parts = unitInfo.split("\\|", -1);
    if (parts.length < 4) return String.valueOf(relativePeriod);
    int spanValue = Integer.parseInt(parts[0]);
    String singular = parts[1];
    String plural = parts[2];

    long absolutePeriod = (baseOffset + relativePeriod - 1) * spanValue;

    String seriesMode = CalcitePlanContext.timewrapSeries.get();
    if (seriesMode == null) seriesMode = "relative";

    return switch (seriesMode) {
      case "short" -> "s" + absolutePeriod;
      case "exact" -> "s" + absolutePeriod; // TODO: format with time_format
      default -> {
        if (absolutePeriod == 0) {
          yield "latest_" + singular;
        } else if (absolutePeriod > 0) {
          String unit = absolutePeriod == 1 ? singular : plural;
          yield absolutePeriod + unit + "_before";
        } else {
          long absPeriod = Math.abs(absolutePeriod);
          String unit = absPeriod == 1 ? singular : plural;
          yield absPeriod + unit + "_after";
        }
      }
    };
  }

  /** Registers opensearch-dependent functions */
  private void registerOpenSearchFunctions() {
    Optional<NodeClient> nodeClient = client.getNodeClient();
    if (nodeClient.isPresent()) {
      SqlUserDefinedFunction geoIpFunction =
          new GeoIpFunction(nodeClient.get()).toUDF(BuiltinFunctionName.GEOIP.name());
      PPLFuncImpTable.INSTANCE.registerExternalOperator(BuiltinFunctionName.GEOIP, geoIpFunction);
      OperatorTable.addOperator(BuiltinFunctionName.GEOIP.name(), geoIpFunction);
    } else {
      logger.info(
          "Function [GEOIP] not registered: incompatible client type {}",
          client.getClass().getName());
    }

    SqlUserDefinedAggFunction approxDistinctCountFunction =
        UserDefinedFunctionUtils.createUserDefinedAggFunction(
            DistinctCountApproxAggFunction.class,
            BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(),
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            null);
    PPLFuncImpTable.INSTANCE.registerExternalAggOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX, approxDistinctCountFunction);
    OperatorTable.addOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(), approxDistinctCountFunction);

    // Note: GraphLookup is now implemented as a custom RelNode (LogicalGraphLookup)
    // instead of a UDF, so no registration is needed here.
  }

  /**
   * Dynamic SqlOperatorTable that allows adding operators after initialization. Similar to
   * PPLBuiltinOperator.instance() or SqlStdOperatorTable.instance().
   */
  public static class OperatorTable extends ListSqlOperatorTable {
    private static final Supplier<OperatorTable> INSTANCE =
        Suppliers.memoize(() -> (OperatorTable) new OperatorTable().init());
    // Use map instead of list to avoid duplicated elements if the class is initialized multiple
    // times
    private static final Map<String, SqlOperator> operators = new ConcurrentHashMap<>();

    public static SqlOperatorTable instance() {
      return INSTANCE.get();
    }

    private ListSqlOperatorTable init() {
      setOperators(buildIndex(operators.values()));
      return this;
    }

    public static synchronized void addOperator(String name, SqlOperator operator) {
      operators.put(name, operator);
    }
  }
}

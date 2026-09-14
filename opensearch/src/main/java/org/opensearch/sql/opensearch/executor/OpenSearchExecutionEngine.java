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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
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
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.TimewrapPivot;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.ResourceLimitExceededException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.UpdateMode;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.transport.client.node.NodeClient;
import tools.jackson.databind.ObjectMapper;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final int DEFAULT_PROGRESSIVE_PREVIEW_BATCH_SIZE = 200;
  private static final long PROGRESSIVE_PREVIEW_INTERVAL_NANOS = 500_000_000L;
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

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

  private Hook.Closeable getOptimizedPlanInHook(AtomicReference<RelNode> optimizedPlan) {
    return getOptimizedPlanInHook(optimizedPlan, ignored -> {});
  }

  private Hook.Closeable getOptimizedPlanInHook(
      AtomicReference<RelNode> optimizedPlan, Consumer<RelNode> onPlan) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        (java.util.function.Consumer<Object>)
            obj -> {
              RelNode physicalPlan = ((RelRoot) obj).rel;
              optimizedPlan.set(physicalPlan);
              onPlan.accept(physicalPlan);
            });
  }

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  /**
   * Parse sourceBuilder JSON strings within the physical plan tree to objects. This finds any
   * sourceBuilder fields (which are serialized as JSON strings by RelJsonWriter) and parses them to
   * JSON objects for easier client consumption.
   */
  @SuppressWarnings("unchecked")
  private void parseSourceBuilderInPhysicalTree(Object physicalTree) {
    try {
      if (!(physicalTree instanceof Map)) {
        return;
      }
      Map<String, Object> tree = (Map<String, Object>) physicalTree;
      Object relsObj = tree.get("rels");
      if (!(relsObj instanceof List)) {
        return;
      }

      List<Object> rels = (List<Object>) relsObj;
      for (Object relObj : rels) {
        if (!(relObj instanceof Map)) {
          continue;
        }
        Map<String, Object> rel = (Map<String, Object>) relObj;

        // Parse sourceBuilder if it exists as a JSON string
        Object sourceBuilderObj = rel.get("sourceBuilder");
        if (sourceBuilderObj instanceof String) {
          try {
            String sourceBuilderJson = (String) sourceBuilderObj;
            Object parsed = objectMapper.readValue(sourceBuilderJson, Object.class);
            rel.put("sourceBuilder", parsed);
          } catch (Exception e) {
            logger.debug("Failed to parse sourceBuilder JSON: {}", e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to parse sourceBuilder in physical tree: " + e.getMessage());
    }
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    explain(rel, mode, null, context, listener);
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      Format format,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (format == Format.JSON_TREE) {
              // Use RelJsonWriter for structured JSON tree output
              try {
                RelJsonWriter logicalWriter = new RelJsonWriter();
                rel.explain(logicalWriter);
                String logicalJson = logicalWriter.asString();

                AtomicReference<String> physicalJson = new AtomicReference<>();
                AtomicReference<Exception> physicalError = new AtomicReference<>();
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;

                try (Hook.Closeable closeable =
                    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
                        obj -> {
                          try {
                            RelRoot relRoot = (RelRoot) obj;
                            RelJsonWriter physicalWriter = new RelJsonWriter();
                            relRoot.rel.explain(physicalWriter);
                            physicalJson.set(physicalWriter.asString());
                          } catch (Exception e) {
                            physicalError.set(e);
                          }
                        })) {
                  // triggers the hook
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }

                if (physicalError.get() != null) {
                  throw physicalError.get();
                }

                // Parse JSON strings to objects for structured output
                Object logicalTree = objectMapper.readValue(logicalJson, Object.class);
                Object physicalTree = objectMapper.readValue(physicalJson.get(), Object.class);

                // Parse sourceBuilder JSON if present in physical plan
                parseSourceBuilderInPhysicalTree(physicalTree);

                ExplainResponseNodeV2 response =
                    new ExplainResponseNodeV2(logicalJson, physicalJson.get(), null);
                response.setLogicalTree(logicalTree);
                response.setPhysicalTree(physicalTree);

                listener.onResponse(new ExplainResponse(response));
              } catch (Exception e) {
                // RelJsonWriter can't handle some custom types (e.g., SystemLimitType enum)
                listener.onFailure(
                    new UnsupportedOperationException(
                        "Cannot serialize plan to json_tree format: " + e.getMessage(), e));
                return;
              }
            } else {
              // Original string format for json/yaml
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
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }
                listener.onResponse(
                    new ExplainResponse(
                        new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
              }
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
          AtomicReference<RelNode> optimizedPlan = new AtomicReference<>();
          ProgressiveQueryResponseListener progressiveListener =
              listener instanceof ProgressiveQueryResponseListener
                  ? (ProgressiveQueryResponseListener) listener
                  : null;
          QueryProgressObserver progressObserver =
              progressiveListener == null ? null : new QueryProgressObserver(progressiveListener);
          ProgressiveQueryContext.Scope progressiveScope =
              progressiveListener == null ? null : ProgressiveQueryContext.open(progressObserver);
          try (progressiveScope;
              Hook.Closeable closeable =
                  getOptimizedPlanInHook(
                      optimizedPlan,
                      physicalPlan -> {
                        if (progressObserver != null) {
                          progressObserver.enableSourceProgress(
                              ProgressiveSourceProgress.create(physicalPlan));
                        }
                      });
              PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
            QueryResponse response;
            try (ProfileScope executePhase = ProfileScope.open(MetricName.EXECUTE)) {
              RelNode physicalPlan = optimizedPlan.get();
              int previewBatchSize = 0;
              if (progressiveListener != null) {
                UpdateMode updateMode =
                    isStablePrefixQuery(rel, physicalPlan) ? UpdateMode.APPEND : UpdateMode.REPLACE;
                progressiveListener.onQueryClassified(updateMode);
                if (updateMode == UpdateMode.APPEND) {
                  previewBatchSize = stablePrefixBatchSize(physicalPlan);
                } else if (supportsAggregationSnapshots(physicalPlan)) {
                  progressObserver.enableAggregationSnapshots(physicalPlan.getRowType());
                } else if (supportsCompositePartialResults(physicalPlan)) {
                  previewBatchSize = DEFAULT_PROGRESSIVE_PREVIEW_BATCH_SIZE;
                }
              }
              ResultSet result = statement.executeQuery();
              response =
                  buildResultSet(
                      result,
                      rel.getRowType(),
                      context.sysLimit.querySizeLimit(),
                      listener,
                      previewBatchSize);
            }
            listener.onResponse(response);
          } catch (SQLException e) {
            if (isPitContextLimitReached(e)) {
              // reason (title) comes from the wrapped cause's message; keep it short and put the
              // explanation and remedy in details.
              ResourceLimitExceededException pitException =
                  new ResourceLimitExceededException(
                      "Too many open Point-In-Time (PIT) contexts on this node.", e);
              throw ErrorReport.wrap(pitException)
                  .code(ErrorCode.RESOURCE_LIMIT_EXCEEDED)
                  .details(
                      "This query opened a Point-In-Time (PIT) context on each shard and reached"
                          + " the limit set by [search.max_open_pit_context]. Increase that"
                          + " setting.")
                  .build();
            }
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Substring of the error OpenSearch's {@code SearchService} raises when a node has no free PIT
   * contexts. The engine opens a PIT (one reader context per shard) to page over a query it cannot
   * push down -- e.g. a {@code stats} that groups by a text field with no {@code keyword} sub-field
   * -- and a busy node exhausts its per-node budget. The raw failure is an opaque internal message,
   * so it is replaced with an actionable one when this marker appears anywhere in the cause chain.
   */
  private static final String PIT_CONTEXT_LIMIT_MARKER = "too many Point In Time contexts";

  /** Package-private for testing. Walks the cause chain guarding against self-referential loops. */
  static boolean isPitContextLimitReached(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.contains(PIT_CONTEXT_LIMIT_MARKER)) {
        return true;
      }
    }
    return false;
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
      ResultSet resultSet,
      RelDataType rowTypes,
      Integer querySizeLimit,
      ResponseListener<QueryResponse> listener,
      int previewBatchSize)
      throws SQLException {
    // Get the ResultSet metadata to know about columns
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();
    int nextPreviewSize = previewBatchSize;
    int lastPreviewSize = 0;
    long lastPreviewNanos = System.nanoTime();
    // Iterate through the ResultSet
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      if (OpenSearchQueryManager.getCancellableTask() != null
          && OpenSearchQueryManager.getCancellableTask().isCancelled()) {
        throw new TaskCancelledException(
            OpenSearchQueryManager.getCancellableTask().getReasonCancelled());
      }
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
      long now = System.nanoTime();
      boolean sizeThresholdReached = previewBatchSize > 0 && values.size() >= nextPreviewSize;
      boolean timeThresholdReached =
          previewBatchSize > 0
              && values.size() > lastPreviewSize
              && now - lastPreviewNanos >= PROGRESSIVE_PREVIEW_INTERVAL_NANOS;
      if (sizeThresholdReached || timeThresholdReached) {
        Schema previewSchema = buildSchema(metaData, fieldTypes, values);
        QueryResponse preview = new QueryResponse(previewSchema, List.copyOf(values), null);
        publishPartial((ProgressiveQueryResponseListener) listener, preview);
        lastPreviewSize = values.size();
        lastPreviewNanos = now;
        nextPreviewSize =
            (int)
                Math.min(
                    Integer.MAX_VALUE,
                    Math.max((long) nextPreviewSize * 2, (long) values.size() + previewBatchSize));
      }
    }

    List<Column> columns = buildColumns(metaData, fieldTypes, values);
    // Timewrap post-processing: pivot unpivoted rows into period columns. The pivot is shared with
    // the analytics route (AnalyticsExecutionEngine) so both engines produce identical output.
    if (TimewrapPivot.isTimewrap()) {
      try {
        TimewrapPivot.Result pivoted =
            TimewrapPivot.pivot(
                columns,
                values,
                CalcitePlanContext.timewrapUnitName.get(),
                CalcitePlanContext.timewrapSeries.get());
        columns = pivoted.columns();
        values = pivoted.values();
      } finally {
        CalcitePlanContext.clearTimewrapSignals();
      }
    }

    Schema schema = new Schema(columns);
    QueryResponse response = new QueryResponse(schema, values, null);
    response.setWarnings(CalcitePlanContext.drainWarnings());
    return response;
  }

  private static Schema buildSchema(
      ResultSetMetaData metaData, List<RelDataType> fieldTypes, List<ExprValue> values)
      throws SQLException {
    return new Schema(buildColumns(metaData, fieldTypes, values));
  }

  private static Schema buildSchema(RelDataType rowType, List<ExprValue> values) {
    List<Column> columns = new ArrayList<>(rowType.getFieldCount());
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType;
      if (field.getType().getSqlTypeName() == SqlTypeName.ANY) {
        ExprValue value =
            values.isEmpty() ? null : values.getFirst().tupleValue().get(field.getName());
        exprType = value == null ? ExprCoreType.UNDEFINED : value.type();
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      }
      columns.add(new Column(field.getName(), null, exprType));
    }
    return new Schema(columns);
  }

  private static List<Column> buildColumns(
      ResultSetMetaData metaData, List<RelDataType> fieldTypes, List<ExprValue> values)
      throws SQLException {
    int columnCount = metaData.getColumnCount();
    List<Column> columns = new ArrayList<>(columnCount);
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
    return columns;
  }

  private static void publishPartial(
      ProgressiveQueryResponseListener listener, QueryResponse response) {
    listener.onPartial(response);
  }

  private static void publishPartial(
      ProgressiveQueryResponseListener listener, QueryResponse response, QueryProgress progress) {
    if (progress == null) {
      listener.onPartial(response);
    } else {
      listener.onPartial(response, progress);
    }
  }

  /**
   * Returns whether the PPL logical semantics and optimized physical plan both prove that every
   * emitted row is an immutable prefix row.
   *
   * <p>The logical plan check is intentional: a PPL aggregation remains progress-only even when
   * optimization pushes it completely into an OpenSearch DSL aggregation.
   */
  static boolean isStablePrefixQuery(RelNode logicalPlan, RelNode physicalPlan) {
    return !containsBlockingOrAggregation(logicalPlan)
        && !containsBlockingOrAggregation(physicalPlan)
        && stablePrefixBatchSize(physicalPlan) > 0;
  }

  private static boolean containsBlockingOrAggregation(RelNode rel) {
    if (rel == null) {
      return true;
    }
    if (rel instanceof Aggregate
        || isBlockingSort(rel)
        || rel instanceof Window
        || rel instanceof Join
        || rel instanceof SetOp) {
      return true;
    }
    if (rel instanceof TableScan) {
      return false;
    }
    return !(rel instanceof SingleRel && isRowLocalUnary(rel))
        || containsBlockingOrAggregation(rel.getInput(0));
  }

  /**
   * Returns the snapshot batch size when the optimized plan can safely expose finalized root rows
   * as progressive previews. Blocking or multi-input operators disable previews.
   */
  static int stablePrefixBatchSize(RelNode rel) {
    if (rel == null || TimewrapPivot.isTimewrap()) {
      return 0;
    }
    if (rel instanceof Aggregate
        || isBlockingSort(rel)
        || rel instanceof Window
        || rel instanceof Join
        || rel instanceof SetOp) {
      return 0;
    }
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      var aggSpec = scan.getPushDownContext().getAggSpec();
      return aggSpec == null ? DEFAULT_PROGRESSIVE_PREVIEW_BATCH_SIZE : 0;
    }
    if (rel instanceof SingleRel && isRowLocalUnary(rel)) {
      return stablePrefixBatchSize(rel.getInput(0));
    }
    return 0;
  }

  /**
   * Returns an exact result-row target for an append-safe logical plan.
   *
   * <p>A user {@code head}/{@code limit} supplies the denominator. System limits are ignored, and
   * only row-preserving projections may appear between that limit and the scan. Operators such as
   * filters can produce fewer rows than the limit, so their progress remains indeterminate.
   */
  static int stablePrefixProgressTarget(RelNode rel) {
    return stablePrefixProgressTarget(rel, 0);
  }

  private static int stablePrefixProgressTarget(RelNode rel, int target) {
    if (rel instanceof LogicalSystemLimit systemLimit) {
      return stablePrefixProgressTarget(systemLimit.getInput(), target);
    }
    if (rel instanceof Sort sort) {
      if (isBlockingSort(sort) || !(sort.fetch instanceof RexLiteral fetch)) {
        return 0;
      }
      Integer limit = fetch.getValueAs(Integer.class);
      if (limit == null || limit <= 0) {
        return 0;
      }
      int effectiveTarget = target == 0 ? limit : Math.min(target, limit);
      return stablePrefixProgressTarget(sort.getInput(), effectiveTarget);
    }
    if (rel instanceof Project project) {
      return stablePrefixProgressTarget(project.getInput(), target);
    }
    return rel instanceof TableScan ? target : 0;
  }

  /**
   * Only a single-request pushed-down aggregation can publish one self-contained reduce snapshot.
   */
  static boolean supportsAggregationSnapshots(RelNode rel) {
    if (!(rel instanceof CalciteEnumerableIndexScan scan)) {
      return false;
    }
    var aggSpec = scan.getPushDownContext().getAggSpec();
    return aggSpec != null && !aggSpec.isCompositeAggregation();
  }

  /**
   * A fully pushed composite aggregation emits finalized bucket rows page by page through the root
   * result set. Running publications contain all root rows accumulated so far and therefore use
   * REPLACE.
   */
  static boolean supportsCompositePartialResults(RelNode rel) {
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      var aggSpec = scan.getPushDownContext().getAggSpec();
      return aggSpec != null && aggSpec.isCompositeAggregation();
    }
    return rel instanceof SingleRel
        && isRowLocalUnary(rel)
        && supportsCompositePartialResults(rel.getInput(0));
  }

  static List<ExprValue> orderAggregationRows(RelDataType rowType, List<ExprValue> unorderedRows) {
    return unorderedRows.stream()
        .map(
            row -> {
              Map<String, ExprValue> ordered = new LinkedHashMap<>();
              for (RelDataTypeField field : rowType.getFieldList()) {
                ordered.put(field.getName(), row.tupleValue().get(field.getName()));
              }
              return (ExprValue) ExprTupleValue.fromExprValueMap(ordered);
            })
        .toList();
  }

  /**
   * Calcite represents both ordered sorting and an unordered LIMIT/OFFSET with {@link Sort}.
   * LIMIT/OFFSET preserves the stability of every row it emits; only a non-empty collation can
   * revise earlier output after seeing later input.
   */
  private static boolean isBlockingSort(RelNode rel) {
    return rel instanceof Sort sort && !sort.getCollation().getFieldCollations().isEmpty();
  }

  /** Operators that transform or filter one row without revising previously emitted root rows. */
  private static boolean isRowLocalUnary(RelNode rel) {
    return rel instanceof Project
        || rel instanceof Filter
        || rel instanceof Calc
        || rel instanceof LogicalSystemLimit
        || (rel instanceof Sort && !isBlockingSort(rel));
  }

  private static final class QueryProgressObserver implements ProgressiveQueryContext.Observer {
    private final ProgressiveQueryResponseListener listener;
    private volatile RelDataType aggregationRowType;
    private volatile ProgressiveSourceProgress sourceProgress;

    private QueryProgressObserver(ProgressiveQueryResponseListener listener) {
      this.listener = listener;
    }

    @Override
    public void onProgress(QueryProgress progress) {
      listener.onProgress(progress);
    }

    @Override
    public void onSourceProgress(long sourceId, QueryProgress progress) {
      ProgressiveSourceProgress tracker = sourceProgress;
      listener.onProgress(tracker == null ? progress : tracker.updateSearch(sourceId, progress));
    }

    @Override
    public void onSourceRows(
        long sourceId,
        long completedRows,
        long estimatedTotalRows,
        boolean estimatedTotalExact,
        boolean complete) {
      ProgressiveSourceProgress tracker = sourceProgress;
      if (tracker != null) {
        listener.onProgress(
            tracker.updateRows(
                sourceId, completedRows, estimatedTotalRows, estimatedTotalExact, complete));
      }
    }

    @Override
    public void onSourcePageProgress(
        long sourceId, QueryProgress pageProgress, long expectedPageUnits) {
      ProgressiveSourceProgress tracker = sourceProgress;
      if (tracker != null) {
        listener.onProgress(tracker.updatePage(sourceId, pageProgress, expectedPageUnits));
      }
    }

    private void enableSourceProgress(ProgressiveSourceProgress sourceProgress) {
      if (sourceProgress.hasSources()) {
        this.sourceProgress = sourceProgress;
        listener.onProgress(sourceProgress.current());
      }
    }

    private void enableAggregationSnapshots(RelDataType rowType) {
      aggregationRowType = rowType;
    }

    @Override
    public void onAggregationSnapshot(List<ExprValue> rows) {
      RelDataType rowType = aggregationRowType;
      if (rowType == null || rows.isEmpty()) {
        return;
      }
      List<ExprValue> orderedRows = orderAggregationRows(rowType, rows);
      publishPartial(
          listener,
          new QueryResponse(buildSchema(rowType, orderedRows), List.copyOf(orderedRows), null),
          sourceProgress == null ? QueryProgress.ZERO : sourceProgress.current());
    }

    @Override
    public void onSearchTaskStarted(long operationId, Runnable cancelAction) {
      listener.onSearchTaskStarted(operationId, cancelAction);
    }

    @Override
    public void onSearchTaskFinished(long operationId) {
      listener.onSearchTaskFinished(operationId);
    }
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

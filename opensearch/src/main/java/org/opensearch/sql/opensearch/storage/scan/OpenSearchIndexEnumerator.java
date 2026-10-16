/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.calcite.utils.VectorErrors;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;
import org.opensearch.sql.data.utils.ComparableLinkedHashMap;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprEncodedVectorValue;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.tasks.CancellableTask;

/**
 * Supports a simple iteration over a collection for OpenSearch index
 *
 * <p>Analogous to LINQ's System.Collections.Enumerator. Unlike LINQ, if the underlying collection
 * has been modified it is only optional that an implementation of the Enumerator interface detects
 * it and throws a {@link java.util.ConcurrentModificationException}.
 */
public class OpenSearchIndexEnumerator implements Enumerator<Object> {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  private final BackgroundSearchScanner bgScanner;

  private final List<String> fields;

  /**
   * Per field, the vector type of the column, or null. Storage returns a vector as it was written
   * (JSON numbers, a base64 string or a single number); Calcite expects Float or Byte elements.
   */
  private final List<ExprVectorType> vectorTypes;

  /** The vector fields of the index by path, e.g. {@code obj.vec}, to convert inside columns. */
  private final Map<String, VectorFieldType> vectorFields;

  /**
   * For each object or nested field that contains a vector field, its children that are vector
   * fields or contain one: child name to child path, e.g. {@code obj -> {vec -> obj.vec}}. Built
   * once per scan, so that rows are walked without building paths.
   */
  private final Map<String, Map<String, String>> vectorChildren;

  /** Per field, whether the column holds an object or nested value that may contain vectors. */
  private final List<Boolean> containerColumns;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** How many moveNext() calls to perform resource check once. */
  private static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /** ResourceMonitor. */
  private final ResourceMonitor monitor;

  /** Number of rows returned. */
  private Integer queryCount = 0;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  private ExprValue current = null;

  private CancellableTask cancellableTask;

  /**
   * @param fields the column names, which are the paths of the fields they read
   * @param fieldTypes the Calcite types of {@code fields}, in the same order
   * @param vectorFields the vector fields of the index by path, to convert vectors in object and
   *     nested columns too
   */
  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      List<RelDataType> fieldTypes,
      Map<String, VectorFieldType> vectorFields,
      int maxResponseSize,
      int maxResultWindow,
      int queryBucketSize,
      OpenSearchRequest request,
      ResourceMonitor monitor) {
    org.opensearch.sql.monitor.ResourceStatus status = monitor.getStatus();
    if (!status.isHealthy()) {
      throw new NonFallbackCalciteException(
          String.format(
              "Insufficient resources to start query: %s. "
                  + "To increase the limit, adjust the 'plugins.query.memory_limit' setting "
                  + "(default: 85%%).",
              status.getFormattedDescription()));
    }

    this.fields = fields;
    this.vectorTypes =
        fieldTypes.stream()
            .map(type -> type instanceof ExprVectorType vector ? vector : null)
            .toList();
    this.vectorFields = vectorFields;
    this.vectorChildren = new HashMap<>();
    for (String path : vectorFields.keySet()) {
      for (int dot = path.indexOf('.'); dot > 0; dot = path.indexOf('.', dot + 1)) {
        String parent = path.substring(0, dot);
        int next = path.indexOf('.', dot + 1);
        String child = next < 0 ? path : path.substring(0, next);
        vectorChildren
            .computeIfAbsent(parent, p -> new HashMap<>())
            .put(child.substring(dot + 1), child);
      }
    }
    this.containerColumns =
        fieldTypes.stream()
            .map(
                type ->
                    !(type instanceof ExprVectorType)
                        && (type.getSqlTypeName() == SqlTypeName.MAP
                            || type.getSqlTypeName() == SqlTypeName.ROW
                            || type.getSqlTypeName() == SqlTypeName.ARRAY))
            .toList();
    this.request = request;
    this.maxResponseSize = maxResponseSize;
    this.monitor = monitor;
    this.client = client;
    this.bgScanner = new BackgroundSearchScanner(client, maxResultWindow, queryBucketSize);
    this.bgScanner.startScanning(request);
    this.cancellableTask = OpenSearchQueryManager.getCancellableTask();
  }

  private Iterator<ExprValue> fetchNextBatch() {
    BackgroundSearchScanner.SearchBatchResult result = bgScanner.fetchNextBatch(request);
    return result.iterator();
  }

  @Override
  public Object current() {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}
     */
    if (fields.size() == 1) {
      return resolveForCalcite(current, 0);
    }
    Object[] row = new Object[fields.size()];
    for (int i = 0; i < row.length; i++) {
      row[i] = resolveForCalcite(current, i);
    }
    return row;
  }

  private Object resolveForCalcite(ExprValue value, int index) {
    String rawPath = fields.get(index);
    if (HighlightExpression.HIGHLIGHT_FIELD.equals(rawPath)) {
      ExprValue hl = ExprValueUtils.getTupleValue(value).get(HighlightExpression.HIGHLIGHT_FIELD);
      return (hl != null && !hl.isMissing() && !hl.isNull()) ? hl : null;
    }
    ExprValue resolved = ExprValueUtils.resolveRefPaths(value, List.of(rawPath.split("\\.")));
    ExprVectorType vectorType = vectorTypes.get(index);
    if (vectorType != null) {
      // Only a column of vector type is converted at the top: an aggregation output can be named
      // like a vector field. The column's type also tells how a base64 value is encoded, also for
      // an aggregation output (e.g. first(v)) or a renamed column, which no field is named after.
      return toVector(resolved, false, vectorType, () -> hitIndex(value, rawPath), rawPath);
    }
    if (containerColumns.get(index) && vectorChildren.containsKey(rawPath)) {
      return toCalcite(resolved, rawPath, false, () -> hitIndex(value, rawPath));
    }
    return resolved.valueForCalcite();
  }

  /**
   * The index of the hit a value of {@code column} comes from, as a top_hits aggregation records it
   * in the row (see {@link TopHitsParser#HIT_INDEX_KEY}), or {@code null}. A value read from a
   * search hit carries its index itself (see {@link OpenSearchExprEncodedVectorValue#getIndex}).
   */
  private static @Nullable String hitIndex(ExprValue row, String column) {
    if (!(row instanceof ExprTupleValue tuple)) {
      return null;
    }
    Map<String, ExprValue> values = tuple.tupleValue();
    ExprValue index = values.get(TopHitsParser.HIT_INDEX_KEY + column);
    if (index == null) {
      index = values.get(TopHitsParser.HIT_INDEX_KEY);
    }
    return index == null || index.isNull() || index.isMissing() ? null : index.stringValue();
  }

  /** The value's own index if it has one, else the row's. */
  private static @Nullable String indexOf(ExprValue value, Supplier<@Nullable String> rowIndex) {
    if (value instanceof OpenSearchExprEncodedVectorValue encoded && encoded.getIndex() != null) {
      return encoded.getIndex();
    }
    return rowIndex.get();
  }

  /**
   * The Calcite value of a vector column, with the column's element type. A base64 value is decoded
   * with the column's encoding, unless the indices of the query disagree on whether the elements
   * are bytes: then with the encoding of the value's own index, and if that is not known, it fails
   * rather than give wrong numbers. A number does not depend on the encoding.
   *
   * @param path the column, for the error message
   */
  private static Object toVector(
      ExprValue value,
      boolean inCollection,
      ExprVectorType type,
      Supplier<@Nullable String> rowIndex,
      String path) {
    Object stored = stored(value, inCollection);
    boolean byteElements = type.getDataType().hasByteElements();
    boolean storedAsBytes = byteElements;
    if (stored instanceof String && type.hasMixedElementEncodings()) {
      String index = indexOf(value, rowIndex);
      Boolean bytes = type.storesByteElements(index);
      if (bytes == null) {
        throw VectorErrors.undecodableVector(path, index);
      }
      storedAsBytes = bytes;
    }
    return ExprVectorType.toRuntimeValue(stored, storedAsBytes, byteElements);
  }

  /**
   * The Calcite value of a vector field inside an object or nested column, decoded as {@link
   * #toVector} decodes one, with the field's mapping.
   */
  private static Object toVector(
      ExprValue value,
      boolean inCollection,
      VectorFieldType field,
      Supplier<@Nullable String> rowIndex,
      String path) {
    Object stored = stored(value, inCollection);
    boolean byteElements = field.getDataType().hasByteElements();
    boolean storedAsBytes = byteElements;
    if (stored instanceof String && field.getStoredDataType(null) == null) {
      String index = indexOf(value, rowIndex);
      VectorDataType storedType = index == null ? null : field.getStoredDataType(index);
      if (storedType == null) {
        throw VectorErrors.undecodableVector(path, index);
      }
      storedAsBytes = storedType.hasByteElements();
    }
    return ExprVectorType.toRuntimeValue(stored, storedAsBytes, byteElements);
  }

  /**
   * The Calcite value of {@code value} at {@code path}: what {@code valueForCalcite()} gives
   * (inside a collection, {@code value()}, as a collection converts its elements), except that each
   * vector field in it is converted to Float or Byte elements.
   */
  private Object toCalcite(
      ExprValue value, String path, boolean inCollection, Supplier<@Nullable String> rowIndex) {
    VectorFieldType field = vectorFields.get(path);
    if (field != null) {
      return toVector(value, inCollection, field, rowIndex, path);
    }
    Map<String, String> children = vectorChildren.get(path);
    if (children != null) {
      if (value instanceof ExprTupleValue tuple) {
        // The map class valueForCalcite() and value() give, respectively.
        Map<String, Object> converted =
            inCollection ? new LinkedHashMap<>() : new ComparableLinkedHashMap<>();
        tuple
            .tupleValue()
            .forEach(
                (key, child) -> {
                  String childPath = children.get(key);
                  converted.put(
                      key,
                      childPath != null
                          ? toCalcite(child, childPath, inCollection, rowIndex)
                          : plain(child, inCollection));
                });
        return converted;
      }
      if (value instanceof ExprCollectionValue collection) {
        // A nested field: an array of objects.
        List<Object> converted = new ArrayList<>();
        collection
            .collectionValue()
            .forEach(e -> converted.add(toCalcite(e, path, true, rowIndex)));
        return converted;
      }
    }
    return plain(value, inCollection);
  }

  /** {@code valueForCalcite()}, or inside a collection {@code value()}. */
  private static Object plain(ExprValue value, boolean inCollection) {
    return inCollection ? value.value() : value.valueForCalcite();
  }

  /** A vector as stored: the base64 string or number of an encoded one, else its value. */
  private static Object stored(ExprValue value, boolean inCollection) {
    if (value instanceof OpenSearchExprEncodedVectorValue encoded) {
      return encoded.getStored();
    }
    return inCollection ? value.value() : value.valueForCalcite();
  }

  @Override
  public boolean moveNext() {
    if (queryCount >= maxResponseSize) {
      return false;
    }

    if (cancellableTask != null && cancellableTask.isCancelled()) {
      throw new TaskCancelledException("The task is cancelled.");
    }

    boolean shouldCheck = (queryCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck) {
      org.opensearch.sql.monitor.ResourceStatus status = this.monitor.getStatus();
      if (!status.isHealthy()) {
        throw new NonFallbackCalciteException(
            String.format(
                "Insufficient resources to continue processing query: %s. "
                    + "Rows processed: %d. "
                    + "To increase the limit, adjust the 'plugins.query.memory_limit' setting "
                    + "(default: 85%%).",
                status.getFormattedDescription(), queryCount));
      }
    }

    if (iterator == null || (!iterator.hasNext() && !this.bgScanner.isScanDone())) {
      iterator = fetchNextBatch();
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      queryCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    bgScanner.reset(request);
    iterator = bgScanner.fetchNextBatch(request).iterator();
    queryCount = 0;
  }

  @Override
  public void close() {
    iterator = Collections.emptyIterator();
    queryCount = 0;
    bgScanner.close();
    if (request != null) {
      client.forceCleanup(request);
      request = null;
    }
  }
}

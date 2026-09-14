/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.tasks.CancellableTask;

/**
 * Request-scoped bridge between Calcite execution and ordinary OpenSearch search tasks.
 *
 * <p>Background search tasks explicitly capture and restore this context because executor thread
 * pools do not inherit thread locals.
 */
public final class ProgressiveQueryContext {
  private static final Logger LOG = LogManager.getLogger(ProgressiveQueryContext.class);

  /** How one physical OpenSearch source exposes progress. */
  public enum SearchMode {
    SINGLE_REQUEST,
    PIT_HITS,
    COMPOSITE
  }

  /** Receives search progress and cancellation handles for the owning PPL job. */
  public interface Observer {
    void onProgress(QueryProgress progress);

    default void onSourceProgress(long sourceId, QueryProgress progress) {
      onProgress(progress);
    }

    default void onSourceRows(
        long sourceId,
        long completedRows,
        long estimatedTotalRows,
        boolean estimatedTotalExact,
        boolean complete) {}

    default void onSourcePageProgress(
        long sourceId, QueryProgress pageProgress, long expectedPageUnits) {}

    default void onAggregationSnapshot(List<ExprValue> rows) {}

    void onSearchTaskStarted(long operationId, Runnable cancelAction);

    void onSearchTaskFinished(long operationId);
  }

  /** Captured context that may be propagated to another worker thread. */
  public static final class Captured {
    private final Observer observer;
    private final AtomicLong nextOperationId;
    private final AtomicLong nextSourceId;
    private final Map<Object, List<Long>> sourceIds;
    private final Map<Long, AtomicLong> completedUnits;
    private final long sourceId;

    private Captured(Observer observer) {
      this(
          observer,
          new AtomicLong(),
          new AtomicLong(),
          new IdentityHashMap<>(),
          new ConcurrentHashMap<>(),
          0L);
    }

    private Captured(
        Observer observer,
        AtomicLong nextOperationId,
        AtomicLong nextSourceId,
        Map<Object, List<Long>> sourceIds,
        Map<Long, AtomicLong> completedUnits,
        long sourceId) {
      this.observer = Objects.requireNonNull(observer);
      this.nextOperationId = nextOperationId;
      this.nextSourceId = nextSourceId;
      this.sourceIds = sourceIds;
      this.completedUnits = completedUnits;
      this.sourceId = sourceId;
    }

    public Observer observer() {
      return observer;
    }

    private long nextOperationId() {
      return nextOperationId.incrementAndGet();
    }

    private long registerSourceOccurrence(Object sourceKey) {
      synchronized (sourceIds) {
        long id = nextSourceId.incrementAndGet();
        sourceIds.computeIfAbsent(sourceKey, ignored -> new ArrayList<>()).add(id);
        completedUnits.putIfAbsent(id, new AtomicLong());
        return id;
      }
    }

    private long resolveSource(Object sourceKey, long embeddedSourceId) {
      synchronized (sourceIds) {
        List<Long> ids = sourceIds.get(sourceKey);
        if (ids == null || ids.isEmpty()) {
          return embeddedSourceId;
        }
        return embeddedSourceId > 0L && ids.contains(embeddedSourceId)
            ? embeddedSourceId
            : ids.getFirst();
      }
    }

    private Captured forSource(long sourceId) {
      return new Captured(
          observer, nextOperationId, nextSourceId, sourceIds, completedUnits, sourceId);
    }

    private long addCompletedUnits(long sourceId, long pageUnits) {
      AtomicLong completed = completedUnits.computeIfAbsent(sourceId, ignored -> new AtomicLong());
      return completed.updateAndGet(
          current ->
              pageUnits > 0L && current > Long.MAX_VALUE - pageUnits
                  ? Long.MAX_VALUE
                  : current + Math.max(0L, pageUnits));
    }

    private long completedUnits(long sourceId) {
      AtomicLong completed = completedUnits.get(sourceId);
      return completed == null ? 0L : completed.get();
    }

    public boolean hasSource() {
      return sourceId > 0L;
    }
  }

  /** Restores the previous context when closed. */
  public static final class Scope implements AutoCloseable {
    private final Captured previous;

    private Scope(Captured previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      restore(previous);
    }
  }

  private static final ThreadLocal<Captured> CURRENT = new ThreadLocal<>();

  private ProgressiveQueryContext() {}

  public static Scope open(Observer observer) {
    Captured previous = CURRENT.get();
    CURRENT.set(new Captured(Objects.requireNonNull(observer)));
    return new Scope(previous);
  }

  public static Captured capture() {
    return CURRENT.get();
  }

  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  /** Registers one occurrence of a physical source before execution begins. */
  public static long registerSourceOccurrence(Object sourceKey) {
    Captured captured = CURRENT.get();
    return captured == null
        ? 0L
        : captured.registerSourceOccurrence(Objects.requireNonNull(sourceKey));
  }

  /** Runs source construction under the identity assigned during physical-plan registration. */
  public static <T> T withSource(long sourceId, Object sourceKey, Supplier<T> supplier) {
    Captured captured = CURRENT.get();
    if (captured == null) {
      return supplier.get();
    }
    long resolved = captured.resolveSource(Objects.requireNonNull(sourceKey), sourceId);
    Captured previous = captured;
    try {
      CURRENT.set(captured.forSource(resolved));
      return supplier.get();
    } finally {
      restore(previous);
    }
  }

  /**
   * Starts one underlying OpenSearch search operation.
   *
   * @param mode how this request contributes source progress
   */
  public static SearchOperation startSearch(SearchMode mode) {
    Captured captured = CURRENT.get();
    if (captured == null) {
      return null;
    }
    return new SearchOperation(
        captured.nextOperationId(), Objects.requireNonNull(mode), captured.sourceId, captured);
  }

  /** Marks the currently executing physical source complete. */
  public static void completeSource() {
    Captured captured = CURRENT.get();
    if (captured == null || !captured.hasSource()) {
      return;
    }
    captured
        .observer()
        .onSourceRows(
            captured.sourceId, captured.completedUnits(captured.sourceId), -1L, false, true);
  }

  /** Handle for one normal {@code _search} request. */
  public static final class SearchOperation {
    private final long id;
    private final SearchMode mode;
    private final long sourceId;
    private final Captured captured;
    private final Observer observer;

    private SearchOperation(long id, SearchMode mode, long sourceId, Captured captured) {
      this.id = id;
      this.mode = mode;
      this.sourceId = sourceId;
      this.captured = captured;
      this.observer = captured.observer();
    }

    public SearchMode mode() {
      return mode;
    }

    public boolean usesPageProgress() {
      return mode != SearchMode.SINGLE_REQUEST;
    }

    public void registerTask(CancellableTask task) {
      try {
        observer.onSearchTaskStarted(id, () -> task.cancel("PPL asynchronous job cancelled"));
      } catch (RuntimeException e) {
        LOG.warn("Failed to register an OpenSearch search task", e);
      }
    }

    public void publish(QueryProgress progress) {
      try {
        observer.onSourceProgress(sourceId, progress);
      } catch (RuntimeException e) {
        LOG.warn("Failed to publish OpenSearch search progress", e);
      }
    }

    public void publishPage(long pageUnits, long totalRows, boolean totalExact) {
      try {
        long completed = captured.addCompletedUnits(sourceId, pageUnits);
        observer.onSourceRows(sourceId, completed, totalRows, totalExact, false);
      } catch (RuntimeException e) {
        LOG.warn("Failed to publish OpenSearch page progress", e);
      }
    }

    public void publishPageProgress(QueryProgress progress, long expectedPageUnits) {
      try {
        observer.onSourcePageProgress(sourceId, progress, expectedPageUnits);
      } catch (RuntimeException e) {
        LOG.warn("Failed to publish OpenSearch page progress", e);
      }
    }

    public void publishAggregationSnapshot(List<ExprValue> rows) {
      observer.onAggregationSnapshot(rows);
    }

    public void complete() {
      try {
        observer.onSearchTaskFinished(id);
      } catch (RuntimeException e) {
        LOG.warn("Failed to unregister an OpenSearch search task", e);
      }
    }
  }

  public static <T> T withContext(Captured captured, Supplier<T> supplier) {
    Captured previous = CURRENT.get();
    try {
      restore(captured);
      return supplier.get();
    } finally {
      restore(previous);
    }
  }

  private static void restore(Captured captured) {
    if (captured == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(captured);
    }
  }
}

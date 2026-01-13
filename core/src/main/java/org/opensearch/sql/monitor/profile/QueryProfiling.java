/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Thread-local holder for query profiling contexts.
 *
 * <p>Callers can enable or disable profiling per-thread via {@link #activate(boolean)} and obtain
 * the active context with {@link #current()}.
 */
public final class QueryProfiling {

  private static final ThreadLocal<ProfileContext> CURRENT = new ThreadLocal<>();

  private QueryProfiling() {}

  /**
   * @return the profiling context bound to this thread, or a no-op context if not activated.
   */
  public static ProfileContext current() {
    ProfileContext ctx = CURRENT.get();
    return ctx == null ? NoopProfileContext.INSTANCE : ctx;
  }

  /**
   * Create noop profiling for the current thread.
   *
   * @return newly activated profiling context
   */
  public static ProfileContext noop() {
    return activate(false);
  }

  /**
   * Activate profiling for the current thread.
   *
   * @param profilingEnabled whether profiling should be enabled
   * @return newly activated profiling context
   */
  public static ProfileContext activate(boolean profilingEnabled) {
    if (profilingEnabled) {
      CURRENT.set(new DefaultProfileContext());
    } else {
      CURRENT.set(NoopProfileContext.INSTANCE);
    }
    return CURRENT.get();
  }

  /** Clear any profiling context bound to the current thread. */
  public static void clear() {
    CURRENT.remove();
  }

  /**
   * Run a supplier with the provided profiling context bound to the current thread.
   *
   * @param action supplier to execute
   * @return supplier result
   */
  public static <T> T withCurrentContext(ProfileContext ctx, Supplier<T> action) {
    CURRENT.set(Objects.requireNonNull(ctx, "ctx"));
    try {
      return action.get();
    } finally {
      clear();
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry holding all available dialect plugins. Initialized at startup with built-in dialects.
 *
 * <p>Lifecycle: During plugin initialization, dialects are registered via {@link #register}. Once
 * all built-in dialects are registered, {@link #freeze()} is called to convert the internal map to
 * an immutable copy. After freezing, no new registrations are accepted and all lookups are lock-free
 * via the immutable map.
 *
 * <p>Thread-safety: All public methods are safe for concurrent use. Before freeze, registration is
 * synchronized. After freeze, {@link #resolve} and {@link #availableDialects} are lock-free reads
 * against an immutable map.
 */
public class DialectRegistry {

  private final Map<String, DialectPlugin> mutableDialects = new ConcurrentHashMap<>();
  private volatile Map<String, DialectPlugin> dialects;
  private volatile boolean frozen = false;

  /**
   * Register a dialect plugin. The dialect name is obtained from {@link
   * DialectPlugin#dialectName()}.
   *
   * @param plugin the dialect plugin to register
   * @throws IllegalStateException if the registry has been frozen after initialization
   * @throws IllegalArgumentException if a dialect with the same name is already registered
   */
  public synchronized void register(DialectPlugin plugin) {
    if (frozen) {
      throw new IllegalStateException("Registry is frozen after initialization");
    }
    String name = plugin.dialectName();
    if (mutableDialects.containsKey(name)) {
      throw new IllegalArgumentException("Dialect '" + name + "' is already registered");
    }
    mutableDialects.put(name, plugin);
  }

  /**
   * Freeze the registry after startup. Converts the internal mutable map to an immutable copy for
   * lock-free reads. After this call, {@link #register} will throw {@link IllegalStateException}.
   */
  public synchronized void freeze() {
    this.dialects = Map.copyOf(mutableDialects);
    this.frozen = true;
  }

  /**
   * Returns whether this registry has been frozen.
   *
   * @return true if {@link #freeze()} has been called
   */
  public boolean isFrozen() {
    return frozen;
  }

  /**
   * Resolve a dialect by name. Uses the frozen immutable map if available, otherwise falls back to
   * the mutable map (during initialization).
   *
   * @param dialectName the dialect name to look up
   * @return an {@link Optional} containing the plugin if found, or empty if not registered
   */
  public Optional<DialectPlugin> resolve(String dialectName) {
    Map<String, DialectPlugin> snapshot = this.dialects;
    if (snapshot != null) {
      return Optional.ofNullable(snapshot.get(dialectName));
    }
    return Optional.ofNullable(mutableDialects.get(dialectName));
  }

  /**
   * Returns the set of all registered dialect names.
   *
   * @return an unmodifiable set of the registered dialect names
   */
  public Set<String> availableDialects() {
    Map<String, DialectPlugin> snapshot = this.dialects;
    if (snapshot != null) {
      return snapshot.keySet();
    }
    return Set.copyOf(mutableDialects.keySet());
  }
}

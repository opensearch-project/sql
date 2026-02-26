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
 * Uses a {@link ConcurrentHashMap} for thread-safe registration and lookup.
 */
public class DialectRegistry {

  private final Map<String, DialectPlugin> dialects = new ConcurrentHashMap<>();

  /**
   * Register a dialect plugin. The dialect name is obtained from {@link
   * DialectPlugin#dialectName()}.
   *
   * @param plugin the dialect plugin to register
   * @throws IllegalArgumentException if a dialect with the same name is already registered
   */
  public void register(DialectPlugin plugin) {
    String name = plugin.dialectName();
    DialectPlugin existing = dialects.putIfAbsent(name, plugin);
    if (existing != null) {
      throw new IllegalArgumentException(
          "Dialect '" + name + "' is already registered");
    }
  }

  /**
   * Resolve a dialect by name.
   *
   * @param dialectName the dialect name to look up
   * @return an {@link Optional} containing the plugin if found, or empty if not registered
   */
  public Optional<DialectPlugin> resolve(String dialectName) {
    return Optional.ofNullable(dialects.get(dialectName));
  }

  /**
   * Returns the set of all registered dialect names.
   *
   * @return an unmodifiable view of the registered dialect names
   */
  public Set<String> availableDialects() {
    return Set.copyOf(dialects.keySet());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.esdomain.mapping;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Mappings interface to provide default implementation (minimal set of Map methods) for subclass in
 * hierarchy.
 *
 * @param <T> Type of nested mapping
 */
public interface Mappings<T> {

  default boolean has(String name) {
    return data().containsKey(name);
  }

  default Collection<String> allNames() {
    return data().keySet();
  }

  default T mapping(String name) {
    return data().get(name);
  }

  default T firstMapping() {
    return allMappings().iterator().next();
  }

  default Collection<T> allMappings() {
    return data().values();
  }

  default boolean isEmpty() {
    return data().isEmpty();
  }

  Map<String, T> data();

  /** Build a map from an existing map by applying provided function to each value. */
  default <X, Y> Map<String, Y> buildMappings(Map<String, X> mappings, Function<X, Y> func) {
    return mappings.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(Map.Entry::getKey, func.compose(Map.Entry::getValue)));
  }
}

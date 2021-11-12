/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.esdomain.mapping;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.opensearch.common.collect.ImmutableOpenMap;

/**
 * Mappings interface to provide default implementation (minimal set of Map methods) for subclass in hierarchy.
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

    /**
     * Convert OpenSearch ImmutableOpenMap<String, X> to JDK Map<String, Y> by applying function: Y func(X)
     */
    default <X, Y> Map<String, Y> buildMappings(ImmutableOpenMap<String, X> mappings, Function<X, Y> func) {
        ImmutableMap.Builder<String, Y> builder = ImmutableMap.builder();
        for (ObjectObjectCursor<String, X> mapping : mappings) {
            builder.put(mapping.key, func.apply(mapping.value));
        }
        return builder.build();
    }
}

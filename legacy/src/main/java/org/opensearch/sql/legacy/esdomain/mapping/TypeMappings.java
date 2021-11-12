/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.esdomain.mapping;

import java.util.Map;
import java.util.Objects;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.collect.ImmutableOpenMap;

/**
 * Type mappings in a specific index.
 * <p>
 * Sample:
 * typeMappings: {
 * '_doc': fieldMappings
 * }
 */
public class TypeMappings implements Mappings<FieldMappings> {

    /**
     * Mapping from Type name to mappings of all Fields in it
     */
    private final Map<String, FieldMappings> typeMappings;

    public TypeMappings(ImmutableOpenMap<String, MappingMetadata> mappings) {
        typeMappings = buildMappings(mappings, FieldMappings::new);
    }

    @Override
    public Map<String, FieldMappings> data() {
        return typeMappings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeMappings that = (TypeMappings) o;
        return Objects.equals(typeMappings, that.typeMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeMappings);
    }

    @Override
    public String toString() {
        return "TypeMappings{" + typeMappings + '}';
    }
}

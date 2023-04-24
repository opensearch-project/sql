/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.esdomain.mapping;

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.Objects;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;

/**
 * Index mappings in the cluster.
 * <p>
 * Sample:
 * indexMappings: {
 * 'accounts': typeMappings1,
 * 'logs':     typeMappings2
 * }
 * <p>
 * Difference between response of getMapping/clusterState and getFieldMapping:
 * <p>
 * 1) MappingMetadata:
 * ((Map) ((Map) (mapping.get("bank").get("account").sourceAsMap().get("properties"))).get("balance")).get("type")
 * <p>
 * 2) FieldMetadata:
 * ((Map) client.admin().indices().getFieldMappings(request).actionGet().mappings().get("bank")
 * .get("account").get("balance").sourceAsMap().get("balance")).get("type")
 */
public class IndexMappings implements Mappings<FieldMappings> {

    public static final IndexMappings EMPTY = new IndexMappings();

    /**
     * Mapping from Index name to mappings of all fields in it
     */
    private final Map<String, FieldMappings> indexMappings;

    public IndexMappings() {
        this.indexMappings = emptyMap();
    }

    public IndexMappings(Metadata metaData) {
        this.indexMappings = buildMappings(metaData.indices(),
                indexMetaData -> new FieldMappings(indexMetaData.mapping()));
    }

    public IndexMappings(Map<String, MappingMetadata> mappings) {
        this.indexMappings = buildMappings(mappings, FieldMappings::new);
    }

    @Override
    public Map<String, FieldMappings> data() {
        return indexMappings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexMappings that = (IndexMappings) o;
        return Objects.equals(indexMappings, that.indexMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexMappings);
    }

    @Override
    public String toString() {
        return "IndexMappings{" + indexMappings + '}';
    }
}

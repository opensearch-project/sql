package org.opensearch.graph.index.schema;


public enum MappingIndexType {
    //static index
    STATIC,
    //common general index - unifies all entities under the same physical index
    UNIFIED,
    //time partitioned index
    TIME,
    //internal document which will be flattened to a dot separated key pathe
    NESTED
}

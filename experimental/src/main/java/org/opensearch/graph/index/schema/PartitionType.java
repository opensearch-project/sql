package org.opensearch.graph.index.schema;


public enum PartitionType {
    //a complete index
    INDEX,
    // a child (nested document)
    CHILD,
    //embedded document - flattened according to (elastic) dot pattern
    EMBEDDED
}

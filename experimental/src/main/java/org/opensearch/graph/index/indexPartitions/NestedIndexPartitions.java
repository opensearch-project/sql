package org.opensearch.graph.index.indexPartitions;


import javaslang.collection.Stream;

import java.util.Collections;
import java.util.Optional;

public class NestedIndexPartitions implements IndexPartitions {
    //region Constructors
    public NestedIndexPartitions(String...indices) {
        this(Stream.of(indices));
    }

    public NestedIndexPartitions(Iterable<String> indices) {
        this.indices = Stream.ofAll(indices).toJavaList();
    }
    //endregion

    //region IndexPartitions Implementation
    @Override
    public Optional<String> getPartitionField() {
        return Optional.empty();
    }

    @Override
    public Iterable<Partition> getPartitions() {
        return Collections.singletonList(() -> this.indices);
    }
    //endregion

    //region Fields
    private Iterable<String> indices;
    //endregion
}

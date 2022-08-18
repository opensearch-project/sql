package org.opensearch.graph.index.schema;






import java.util.List;

public interface BaseTypeElement<T> {
    List<T> getNested();

    Props getProps();

    String getMapping();

    String getPartition();

    String getType();
}

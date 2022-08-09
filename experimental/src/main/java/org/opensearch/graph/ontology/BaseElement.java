package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.StringJoiner;

public interface BaseElement {
    String ID = "id";

    String getName();

    List<String> getIdField();

    List<String> getMetadata();

    List<String> fields();

    List<String> getProperties();


    @JsonIgnore
    static String idFieldName(List<String> values) {
        StringJoiner joiner = new StringJoiner("_");
        values.forEach(joiner::add);
        return joiner.toString().length() > 0 ?
                joiner.toString() : ID;
    }
}

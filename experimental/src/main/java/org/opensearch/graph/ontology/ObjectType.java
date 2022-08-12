package org.opensearch.graph.ontology;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ObjectType extends PrimitiveType{

    ObjectType() {
        super();
    }
    public ObjectType(String type) {
        this(type,Object.class);
    }
    public ObjectType(String type, Class javaType) {
        super(type, javaType);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ArrayOfObjects extends ObjectType {

        ArrayOfObjects() {
            super();
        }

        public ArrayOfObjects(String type) {
            super(type);
        }

        public ArrayOfObjects(String type, Class javaType) {
            super(type, javaType);
        }
    }
}

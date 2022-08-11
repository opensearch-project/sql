package org.opensearch.graph.ontology;


import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class PrimitiveType {
    private String type;
    private Class javaType;

    public PrimitiveType(String type, Class javaType) {
        this.type = type;
        this.javaType = javaType;
    }

    public String getType() {
        return type;
    }

    public Class getJavaType() {
        return javaType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimitiveType that = (PrimitiveType) o;
        return getType().equals(that.getType()) &&
                getJavaType().equals(that.getJavaType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getJavaType());
    }

    /* Array of primitives */
    public static class ArrayOfPrimitives extends PrimitiveType {

        public ArrayOfPrimitives(String type, Class javaType) {
            super(type, javaType);
        }
    }

    /**
     * default primitive types
     */
    public enum Types {
        ID, BOOLEAN, LIST_OF_BOOLEAN, INT, LIST_OF_INT, LONG, LIST_OF_LONG, STRING, LIST_OF_STRING, TEXT, FLOAT,
        LIST_OF_FLOAT, TIME, LIST_OF_TIME, DATE, LIST_OF_DATE, DATETIME, LIST_OF_DATETIME, IP, LIST_OF_IP,
        GEOPOINT, LIST_OF_GEOPOINT, JSON, LIST_OF_JSON, ARRAY;

        /**
         * to lower case
         *
         * @return
         */
        public String tlc() {
            return this.name().toLowerCase();
        }

        public static boolean contains(String term) {
            return Arrays.stream(Types.values()).anyMatch(p -> p.tlc().equalsIgnoreCase(term));
        }

        public static Types listOf(String type) {
            return find("LIST_OF_" + find(type).orElse(STRING)).orElse(LIST_OF_STRING);
        }

        public static Optional<Types> find(String term) {
            return Arrays.stream(Types.values()).filter(p -> p.tlc().equalsIgnoreCase(term)).findAny();
        }
    }
}

package org.opensearch.graph.ontology;


import java.util.Arrays;
import java.util.Objects;

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
    public class ArrayOfPrimitives extends PrimitiveType {

        public ArrayOfPrimitives(String type, Class javaType) {
            super(type, javaType);
        }
    }

    /**
     * default primitive types
     */
    public enum Types {
        ID, BOOLEAN, INT, LONG, STRING, TEXT, FLOAT, TIME, DATE, DATETIME, IP, GEOPOINT, ARRAY, JSON;

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
    }
}

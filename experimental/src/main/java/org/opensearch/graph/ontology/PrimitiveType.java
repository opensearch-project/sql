package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class PrimitiveType implements PropertyType {
    private String type;
    @JsonIgnore
    private Class javaType;

    PrimitiveType() {
    }

    public PrimitiveType(String type, Class javaType) {
        this.type = type;
        this.javaType = javaType;
    }

    public String getType() {
        return type;
    }

    @JsonIgnore
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

    @Override
    public String toString() {
        return "PrimitiveType{" +
                "type='" + type + '\'' +
                ", javaType=" + javaType +
                '}';
    }

    /* Array of primitives */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ArrayOfPrimitives extends PrimitiveType {

        ArrayOfPrimitives() {
            super();
        }

        public ArrayOfPrimitives(String type, Class javaType) {
            super(type, javaType);
        }

        public ArrayOfPrimitives(PropertyType type) {
            super(((PrimitiveType) type).type, ((PrimitiveType) type).javaType);
        }
    }

    /**
     * default primitive types
     */
    public enum Types {
        ID, BOOLEAN, INT, LONG, STRING, TEXT, FLOAT,
        TIME, DATE, DATETIME, IP,
        GEOPOINT, JSON, ARRAY;

        /**
         * to lower case
         *
         * @return
         */
        public String tlc() {
            return this.name().toLowerCase();
        }

        public PropertyType asType() {
            return new PrimitiveType(this.name(), this.getClass());
        }

        public PropertyType asListType() {
            return new ArrayOfPrimitives(this.name(), this.getClass());
        }

        public static boolean contains(String term) {
            return Arrays.stream(Types.values()).anyMatch(p -> p.tlc().equalsIgnoreCase(term));
        }

        public static Optional<Types> find(String term) {
            return Arrays.stream(Types.values()).filter(p -> p.tlc().equalsIgnoreCase(term)).findAny();
        }
    }
}

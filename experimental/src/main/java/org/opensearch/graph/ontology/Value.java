package org.opensearch.graph.ontology;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Value {

    public Value() {
    }

    public Value(int val, String name) {
        this.val = val;
        this.name = name;
    }

    public int getVal() {
        return val;
    }

    public void setVal(int val) {
        this.val = val;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return "Value [val = "+val+", name = "+name+"]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Value value = (Value) o;
        return val == value.val &&
                name.equals(value.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(val, name);
    }

    //region Fields
    private int val;
    private String name;
    //endregion

    public static final class ValueBuilder {
        private int val;
        private String name;

        private ValueBuilder() {
        }

        public static ValueBuilder aValue() {
            return new ValueBuilder();
        }

        public ValueBuilder withVal(int val) {
            this.val = val;
            return this;
        }

        public ValueBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public Value build() {
            return new Value(val,name);
        }
    }
}

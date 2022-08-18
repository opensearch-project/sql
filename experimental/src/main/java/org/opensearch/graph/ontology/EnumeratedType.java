package org.opensearch.graph.ontology;







import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.*;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class EnumeratedType {
    public static final String TYPE = "TYPE_";

    public EnumeratedType() {
    }

    public EnumeratedType(String eType, List<Value> values) {
        this.eType = eType;
        this.values = values;
    }

    public String geteType() {
        return eType;
    }

    public void seteType(String eType) {
        this.eType = eType;
    }

    public List<Value> getValues() {
        return values;
    }

    public void setValues(List<Value> values) {
        this.values = values;
    }

    @JsonIgnore
    public Optional<Value> valueOf(String name) {
        return getValues().stream().filter(v -> v.getName().equals(name)).findFirst();
    }

    @JsonIgnore
    public boolean isOfType(String name) {
        return eType.equalsIgnoreCase(name) || eType.equalsIgnoreCase(TYPE +name);
    }

    @JsonIgnore
    public Optional<Value> nameOf(int index) {
        return getValues().stream().filter(v -> v.getVal() == index).findFirst();
    }

    @Override
    public String toString() {
        return "EnumeratedType [values = " + values + ", eType = " + eType + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnumeratedType that = (EnumeratedType) o;
        return eType.equals(that.eType) &&
                values.equals(that.values);
    }

    @Override
    protected EnumeratedType clone()  {
        return new EnumeratedType(this.eType,new ArrayList<>(this.values));
    }

    @Override
    public int hashCode() {
        return Objects.hash(eType, values);
    }

    //region Fields
    private String eType;
    private List<Value> values;
    //endregion

    public static EnumeratedType from(String name, Enum[] enums) {
        return new EnumeratedType(name, Arrays.stream(enums).map(v -> new Value(v.ordinal(), v.name())).collect(Collectors.toList()));
    }


    public static final class EnumeratedTypeBuilder {
        private String eType;
        private List<Value> values;

        private EnumeratedTypeBuilder() {
        }

        public static EnumeratedTypeBuilder anEnumeratedType() {
            return new EnumeratedTypeBuilder();
        }

        public EnumeratedTypeBuilder withEType(String eType) {
            this.eType = eType;
            return this;
        }

        public EnumeratedTypeBuilder withValues(List<Value> values) {
            this.values = values;
            return this;
        }

        public EnumeratedTypeBuilder values(List<String> values) {
            this.values = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                this.values.add(new Value(i, values.get(i)));
            }
            return this;
        }

        public EnumeratedType build() {
            return new EnumeratedType(this.eType, values);
        }
    }


}

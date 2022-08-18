package org.opensearch.graph.ontology;








import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Property {
    //region Constructors
    public Property() {
    }

    public Property(Property clone) {
        this.name = clone.name;
        this.pType = clone.pType;
        this.type = clone.type;
    }

    public Property(String name, String pType, PropertyType type) {
        this.pType = pType;
        this.name = name;
        this.type = type;
    }

    public Property(String name, String pType, PropertyType type, SearchType... searchType) {
        this.pType = pType;
        this.name = name;
        this.type = type;
        this.searchType = Arrays.asList(searchType);
    }
    //endregion

    //region Properties
    public String getpType() {
        return pType;
    }

    public void setpType(String pType) {
        this.pType = pType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PropertyType getType() {
        return type;
    }

    public void setSearchType(List<SearchType> searchType) {
        this.searchType = searchType;
    }

    public void setType(PropertyType type) {
        this.type = type;
    }

    public List<SearchType> getSearchType() {
        return searchType;
    }

    //endregion

    //region Override Methods
    @Override
    public String toString() {
        return String.format("Property [pType = %s, name = %s, type = %s]", this.pType, this.name, this.type);
    }

    @Override
    protected Property clone()  {
        return new Property(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !Property.class.isAssignableFrom(o.getClass())) return false;
        Property property = (Property) o;
        return pType.equals(property.pType) &&
                name.equals(property.name) &&
                type.equals(property.type) &&
                Objects.equals(searchType, property.searchType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pType, name, type, searchType);
    }

    /**
     * check equality by not using the class type
     * @param source
     * @param other
     * @return
     */
    public static boolean equal(Property source,Property other) {
        return source.pType.equals(other.pType) &&
                source.name.equals(other.name) &&
                source.type.equals(other.type);
    }

    //endregion

    //region Fields
    private String pType;
    private String name;
    private PropertyType type;
    private List<SearchType> searchType = Collections.emptyList();
    //endregion

    //region Builder
    public static final class Builder {
        private List<SearchType> searchTypes = new ArrayList<>();
        private String pType;
        private String name;
        private PropertyType type;

        private Builder() {
        }

        public static Builder get() {
            return new Builder();
        }

        public Builder withPType(String pType) {
            this.pType = pType;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withSearchType(SearchType type) {
            this.searchTypes.add(type);
            return this;
        }

        public Builder withType(PropertyType type) {
            this.type = type;
            return this;
        }

        public Property build(String pType, String name, PropertyType type) {
            Property property = new Property();
            property.setName(name);
            property.setType(type);
            property.setpType(pType);
            return property;
        }

        public Property build() {
            Property property = new Property();
            property.setName(name);
            property.setType(type);
            property.pType = this.pType;
            property.searchType = Collections.unmodifiableList(this.searchTypes);
            return property;
        }
    }
    //endregion


    public enum SearchType {
        NGRAM("ngram"),
        PREFIX("prefix"),
        SUFFIX("suffix"),
        FULL("full"),
        EXACT("exact"),
        RANGE("range"),
        NONE("none");

        private String name;

        SearchType(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return name;
        }
    }

    /**
     * mandatory property name holder
     */
    public static class MandatoryProperty extends Property {

        public MandatoryProperty(Property property) {
            super(property);
        }

        public static Optional<Property> of(Optional<Property> property) {
            return property.map(MandatoryProperty::new);
        }
    }

}

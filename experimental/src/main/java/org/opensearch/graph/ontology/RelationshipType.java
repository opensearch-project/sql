package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RelationshipType implements BaseElement {
    public RelationshipType() {
        properties = new ArrayList<>();
        metadata = new ArrayList<>();
        metadata = new ArrayList<>();
        ePairs = new ArrayList<>();
    }

    public RelationshipType(String name, String rType, boolean directional) {
        this();
        this.rType = rType;
        this.name = name;
        this.directional = directional;
        this.mandatory = new ArrayList<>();

    }

    //region Getters & Setters
    public String getrType() {
        return rType;
    }

    public void setrType(String rType) {
        this.rType = rType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDirectional() {
        return directional;
    }

    public void setDirectional(boolean directional) {
        this.directional = directional;
    }

    @JsonProperty("DBrName")
    public String getDbRelationName() {
        return dbRelationName;
    }

    @JsonProperty("DBrName")
    public void setDbRelationName(String dbRelationName) {
        this.dbRelationName = dbRelationName;
    }

    public void setePairs(List<EPair> ePairs) {
        this.ePairs = ePairs;
    }

    public List<EPair> getePairs() {
        return ePairs;
    }

    @JsonIgnore
    public Set<String> getSidesA() {
        return ePairs.stream().map(EPair::geteTypeA).collect(Collectors.toSet());
    }

    @JsonIgnore
    public Set<String> getSidesB() {
        return ePairs.stream().map(EPair::geteTypeB).collect(Collectors.toSet());
    }

    @JsonIgnore
    public RelationshipType addPair(EPair pair) {
        this.getePairs().add(pair);
        return this;
    }

    public List<String> getMetadata() {
        return metadata != null ? metadata : Collections.emptyList();
    }

    @JsonIgnore
    public RelationshipType withMetadata(List<String> metadata) {
        this.metadata.addAll(metadata);
        return this;
    }

    @Override
    protected RelationshipType clone() {
        RelationshipType relationshipType = new RelationshipType();
        relationshipType.directional = this.directional;
        relationshipType.dbRelationName = this.dbRelationName;
        relationshipType.rType = this.rType;
        relationshipType.name = this.name;
        relationshipType.properties = new ArrayList<>(this.properties);
        relationshipType.mandatory = new ArrayList<>(this.mandatory);
        relationshipType.metadata = new ArrayList<>(this.metadata);
        relationshipType.idField = new ArrayList<>(this.idField);
        relationshipType.directives = new ArrayList<>(this.directives);
        relationshipType.ePairs = this.ePairs.stream().map(EPair::clone).collect(Collectors.toList());
        return relationshipType;
    }

    public List<String> getIdField() {
        return idField;
    }

    public void setIdField(List<String> idField) {
        this.idField = idField;
    }

    public void setMetadata(List<String> metadata) {
        this.metadata = metadata;
    }

    public List<String> getProperties() {
        return properties != null ? properties : Collections.emptyList();
    }

    @JsonIgnore
    public RelationshipType withProperties(List<String> properties) {
        this.properties.addAll(properties);
        return this;
    }

    public List<String> getMandatory() {
        return mandatory != null ? mandatory : Collections.emptyList();
    }

    public void setMandatory(List<String> mandatory) {
        this.mandatory = mandatory;
    }

    public void setProperties(List<String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public List<String> fields() {
        return Stream.concat(properties.stream(), metadata.stream()).collect(Collectors.toList());
    }

    @JsonIgnore
    public RelationshipType addProperty(String type) {
        this.properties.add(type);
        return this;
    }

    @JsonIgnore
    public RelationshipType withProperty(String... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    @JsonIgnore
    public RelationshipType addMetadata(String type) {
        this.metadata.add(type);
        return this;
    }

    @JsonIgnore
    public RelationshipType withMetadata(String... properties) {
        this.metadata.addAll(Arrays.asList(properties));
        return this;
    }

    @JsonIgnore
    public RelationshipType withEPairs(EPair... pairs) {
        this.setePairs(Arrays.asList(pairs));
        return this;
    }


    //endregion


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipType that = (RelationshipType) o;
        return directional == that.directional &&
                idField.equals(that.idField) &&
                rType.equals(that.rType) &&
                name.equals(that.name) &&
                mandatory.equals(that.mandatory) &&
                ePairs.equals(that.ePairs) &&
                properties.equals(that.properties) &&
                metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idField, rType, name, directional, mandatory, ePairs, properties, metadata);
    }

    @Override
    public String toString() {
        return "RelationshipType [name = " + name + ", ePairs = " + ePairs + ", idField = " + idField + ", rType = " + rType + ", directional = " + directional + ", properties = " + properties + ", metadata = " + metadata + ", mandatory = " + mandatory + "]";
    }

    @JsonIgnore
    public String idFieldName() {
        return BaseElement.idFieldName(getIdField());
    }

    //region Fields
    private List<String> idField = Collections.singletonList(ID);
    private String rType;
    private String name;
    private boolean directional;
    private String dbRelationName;
    private List<DirectiveType> directives = new ArrayList<>();
    private List<String> mandatory = new ArrayList<>();
    private List<EPair> ePairs;
    private List<String> properties;
    private List<String> metadata;

    @JsonIgnore
    public boolean containsMetadata(String key) {
        return metadata.contains(key);
    }

    @JsonIgnore
    public void directive(DirectiveType value) {
        directives.add(value);
    }

    @JsonIgnore
    public boolean isMandatory(String key) {
        return mandatory.contains(key);
    }

    @JsonIgnore
    public boolean containsProperty(String key) {
        return properties.contains(key);
    }

    @JsonIgnore
    public boolean hasSideA(String eType) {
        return ePairs.stream().anyMatch(ep -> ep.geteTypeA().equals(eType));
    }

    @JsonIgnore
    public boolean hasSideB(String eType) {
        return ePairs.stream().anyMatch(ep -> ep.geteTypeB().equals(eType));
    }

    @JsonIgnore
    public List<DirectiveType> getDirectives() {
        return directives;
    }

//endregion

    //region Builder
    public static final class Builder {
        private List<String> idField = new ArrayList<>();
        private String rType;
        private String name;
        private boolean directional;
        private String DBrName;
        private List<String> mandatory = new ArrayList<>();
        private List<EPair> ePairs = new ArrayList<>();
        private List<String> properties = new ArrayList<>();
        private List<String> metatada = new ArrayList<>();

        private List<DirectiveType> directives = new ArrayList<>();

        private Builder() {
            idField.add(ID);
        }

        public static Builder get() {
            return new Builder();
        }

        public Builder withRType(String rType) {
            this.rType = rType;
            return this;
        }

        public Builder withDirective(DirectiveType value) {
            this.directives.add(value);
            return this;
        }

        public Builder withDirectives(Collection<DirectiveType> values) {
            this.directives.addAll(values);
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withDirectional(boolean directional) {
            this.directional = directional;
            return this;
        }

        public Builder withMandatory(List<String> mandatory) {
            this.mandatory = mandatory;
            return this;
        }

        public Builder withMandatory(String mandatory) {
            this.mandatory.add(mandatory);
            return this;
        }


        public Builder withDBrName(String DBrName) {
            this.DBrName = DBrName;
            return this;
        }

        public Builder withEPairs(List<EPair> ePairs) {
            this.ePairs = ePairs;
            return this;
        }

        public Builder withEPair(EPair ePair) {
            this.ePairs.add(ePair);
            return this;
        }

        public Builder withProperties(List<String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder withProperty(String property) {
            this.properties.add(property);
            return this;
        }

        public Builder withIdField(String... idField) {
            this.idField = Arrays.asList(idField);
            return this;
        }


        public Builder withMetadata(List<String> metatada) {
            this.metatada = metatada;
            return this;
        }

        public RelationshipType build() {
            RelationshipType relationshipType = new RelationshipType();
            relationshipType.setrType(this.rType);
            relationshipType.setIdField(idField);
            relationshipType.setName(name);
            relationshipType.setDirectional(directional);
            relationshipType.setDbRelationName(DBrName);
            relationshipType.setProperties(properties);
            relationshipType.setMetadata(metatada);
            relationshipType.setMandatory(mandatory);
            relationshipType.setePairs(ePairs);
            relationshipType.directives.addAll(this.directives);
            return relationshipType;
        }
    }
    //endregion

}

package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityType implements BaseElement {
    //region Fields
    private List<String> idField = new ArrayList<>();
    private boolean isAbstract;
    private String eType;
    private String name;
    private List<DirectiveType> directives = new ArrayList<>();
    private List<String> mandatory = new ArrayList<>();
    private List<String> properties = new ArrayList<>();
    private List<String> metadata = new ArrayList<>();
    private List<String> display = new ArrayList<>();
    private List<String> parentType = new ArrayList<>();

    public EntityType() {
    }

    public EntityType(String type, String name, List<String> properties, List<String> metadata) {
        this(type, name, properties, metadata, Collections.emptyList(), Collections.emptyList());
    }

    public EntityType(String type, String name, List<String> properties, List<String> metadata, List<String> mandatory, List<String> parentType) {
        this.eType = type;
        this.name = name;
        this.properties = properties;
        this.metadata = metadata;
        this.mandatory = mandatory;
        this.parentType = parentType;
    }

    public EntityType(String type, String name, List<String> properties) {
        this(type, name, properties, Collections.emptyList());
    }

    public List<String> getMetadata() {
        return metadata != null ? metadata : Collections.emptyList();
    }

    public void setMetadata(List<String> metadata) {
        this.metadata = metadata;
    }

    public boolean isAbstract() {
        return isAbstract;
    }

    public void setAbstract(boolean anAbstract) {
        isAbstract = anAbstract;
    }

    public List<DirectiveType> getDirectives() {
        return directives;
    }

    @JsonIgnore
    public void directive(DirectiveType value) {
        directives.add(value);
    }

    @JsonIgnore
    public EntityType withMetadata(List<String> metadata) {
        this.metadata.addAll(metadata);
        return this;
    }

    public String geteType() {
        return eType;
    }

    public void seteType(String eType) {
        this.eType = eType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getProperties() {
        return properties != null ? properties : Collections.emptyList();
    }

    @JsonIgnore
    public EntityType withProperties(List<String> properties) {
        this.properties.addAll(properties);
        return this;
    }

    @Override
    protected EntityType clone() {
        EntityType entityType = new EntityType();
        entityType.eType = this.eType;
        entityType.name = this.name;
        entityType.isAbstract = this.isAbstract;
        entityType.properties = new ArrayList<>(this.properties);
        entityType.mandatory = new ArrayList<>(this.mandatory);
        entityType.metadata = new ArrayList<>(this.metadata);
        entityType.idField = new ArrayList<>(this.idField);
        entityType.display = new ArrayList<>(this.display);
        entityType.parentType = new ArrayList<>(this.parentType);
        entityType.directives = new ArrayList<>(this.directives);
        return entityType;
    }

    public List<String> getParentType() {
        return parentType != null ? parentType : Collections.emptyList();
    }

    public void setParentType(List<String> parentType) {
        this.parentType = parentType;
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

    public List<String> getDisplay() {
        return display;
    }

    public void setDisplay(List<String> display) {
        this.display = display;
    }

    public List<String> getIdField() {
        return idField;
    }

    public void setIdField(List<String> idField) {
        this.idField = idField;
    }

    @Override
    public String toString() {
        return "EntityType [idField = " + idField + ",eType = " + eType + ",abstract = " + isAbstract + ", name = " + name + ", display = " + display + ", properties = " + properties + ", metadata = " + metadata + ", mandatory = " + mandatory + ", directives = " + directives + "]";
    }

    @JsonIgnore
    public String idFieldName() {
        return BaseElement.idFieldName(getIdField());
    }

    @JsonIgnore
    public List<String> fields() {
        return Stream.concat(properties.stream(), metadata.stream()).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityType that = (EntityType) o;
        return idField.equals(that.idField) &&
                isAbstract == that.isAbstract &&
                eType.equals(that.eType) &&
                Objects.equals(parentType, that.parentType) &&
                name.equals(that.name) &&
                properties.equals(that.properties) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(directives, that.directives) &&
                display.equals(that.display);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idField, eType, isAbstract, parentType, name, properties, metadata, display);
    }


    @JsonIgnore
    public boolean containsMetadata(String key) {
        return metadata.contains(key);
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
    public boolean containsSuperType(String key) {
        return parentType.contains(key);
    }
    //endregion

    //region Builder
    public static final class Builder {
        private List<String> idField = new ArrayList<>();
        private String eType;
        private String name;
        private List<String> mandatory = new ArrayList<>();
        private List<String> properties = new ArrayList<>();
        private List<String> metadata = new ArrayList<>();
        private List<String> display = new ArrayList<>();
        private List<String> parentType = new ArrayList<>();
        private List<DirectiveType> directives = new ArrayList<>();

        private boolean isAbstract = false;

        private Builder() {
            // id field is no longer a default
            //  - if no id field defined for root level entities -> an error should be thrown.
            /* idField.add(ID); */
        }


        public static Builder get() {
            return new Builder();
        }

        @JsonIgnore
        public Builder withIdField(String... idField) {
            //only populate if fields are not empty so that the default GlobalConstants.ID would not vanish
            if (idField.length > 0) this.idField = Arrays.asList(idField);
            return this;
        }

        @JsonIgnore
        public Builder withEType(String eType) {
            this.eType = eType;
            return this;
        }

        @JsonIgnore
        public Builder withParentTypes(List<String> superTypes) {
            this.parentType = superTypes;
            return this;
        }

        @JsonIgnore
        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        @JsonIgnore
        public Builder withProperties(List<String> properties) {
            this.properties = properties;
            return this;
        }

        @JsonIgnore
        public Builder withProperty(String property) {
            this.properties.add(property);
            return this;
        }

        @JsonIgnore
        public Builder withMandatory(List<String> mandatory) {
            this.mandatory = mandatory;
            return this;
        }

        @JsonIgnore
        public Builder withMandatory(String mandatory) {
            this.mandatory.add(mandatory);
            return this;
        }

        @JsonIgnore
        public Builder withMetadata(List<String> metadata) {
            this.metadata = metadata;
            return this;
        }

        @JsonIgnore
        public Builder withDisplay(List<String> display) {
            this.display = display;
            return this;
        }

        @JsonIgnore
        public Builder withParentType(String parent) {
            this.parentType.add(parent);
            return this;
        }

        @JsonIgnore
        public Builder isAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }
        @JsonIgnore
        public Builder withDirective(DirectiveType value) {
            this.directives.add(value);
            return this;
        }
        @JsonIgnore
        public Builder withDirectives(Collection<DirectiveType> values) {
            this.directives.addAll(values);
            return this;
        }

        public EntityType build() {
            EntityType entityType = new EntityType();
            entityType.setName(name);
            entityType.setProperties(properties);
            entityType.setMandatory(mandatory);
            entityType.setMetadata(metadata);
            entityType.setDisplay(display);
            entityType.setParentType(parentType);
            entityType.eType = this.eType;
            entityType.idField = this.idField;
            entityType.isAbstract = this.isAbstract;
            entityType.directives.addAll(this.directives);
            return entityType;
        }
    }
    //endregion

}

package org.opensearch.graph.ontology.mapping;

/*-
 * #%L
 * opengraph-model
 * %%
 * Copyright (C) 2016 - 2022 org.opensearch
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */





import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MappingOntologies {

    private String sourceOntology;
    private String targetOntology;
    private List<EntityType> entityTypes;
    private List<RelationshipType> relationshipTypes;
    private List<Property> properties;

    @JsonProperty("source.ontology")
    public String getSourceOntology() {
        return this.sourceOntology;
    }

    @JsonProperty("source.ontology")
    public void setSourceOntology(String sourceOntology) {
        this.sourceOntology = sourceOntology;
    }

    @JsonProperty("target.ontology")
    public String getTargetOntology() {
        return this.targetOntology;
    }

    @JsonProperty("target.ontology")
    public void setTargetOntology(String targetOntology) {
        this.targetOntology = targetOntology;
    }

    @JsonProperty("entityTypes")
    public List<EntityType> getEntityTypes() {
        return this.entityTypes;
    }

    @JsonProperty("entityTypes")
    public void setEntityTypes(List<EntityType> entityTypes) {
        this.entityTypes = entityTypes;
    }

    @JsonProperty("relationshipTypes")
    public List<RelationshipType> getRelationshipTypes() {
        return this.relationshipTypes;
    }

    @JsonProperty("relationshipTypes")
    public void setRelationshipTypes(List<RelationshipType> relationshipTypes) {
        this.relationshipTypes = relationshipTypes;
    }

    @JsonProperty("properties")
    public List<Property> getProperties() {
        return this.properties;
    }

    @JsonProperty("properties")
    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EntityType {
        private List<String> source;
        private String sourceField;
        private String targetField;
        private String target;

        @JsonProperty("source")
        public List<String> getSource() {
            return this.source;
        }

        @JsonProperty("source")
        public void setSource(List<String> source) {
            this.source = source;
        }


        @JsonProperty("source.field")
        public String getSourceField() {
            return this.sourceField;
        }

        @JsonProperty("source.field")
        public void setSourceField(String sourceField) {
            this.sourceField = sourceField;
        }


        @JsonProperty("target")
        public String getTarget() {
            return this.target;
        }

        @JsonProperty("target")
        public void setTarget(String target) {
            this.target = target;
        }

        @JsonProperty("target.field")
        public String getTargetField() {
            return this.targetField;
        }

        @JsonProperty("target.field")
        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RelationshipType {
        private List<String> source;
        private String sourceField;
        private String targetField;
        private String target;

        @JsonProperty("source")
        public List<String> getSource() {
            return this.source;
        }

        public void setSource(List<String> source) {
            this.source = source;
        }

        @JsonProperty("source.field")
        public String getSourceField() {
            return this.sourceField;
        }

        @JsonProperty("source.field")
        public void setSourceField(String sourceField) {
            this.sourceField = sourceField;
        }


        @JsonProperty("target")
        public String getTarget() {
            return this.target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        @JsonProperty("target.field")
        public String getTargetField() {
            return this.targetField;
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Property {
        private String name;
        private Object value;

    }


}

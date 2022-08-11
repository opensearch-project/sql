package org.opensearch.graph.ontology.transformer;


import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class OntologyTransformer {
    private String ont;
    private List<TransformerEntityType> entityTypes;
    private List<TransformerRelationType> relationTypes;

    public OntologyTransformer() {}

    public OntologyTransformer(String ont, List<TransformerEntityType> entityTypes, List<TransformerRelationType> relationTypes) {
        this.ont = ont;
        this.entityTypes = entityTypes;
        this.relationTypes = relationTypes;
    }

    public String getOnt() {
        return ont;
    }

    public void setOnt(String ont) {
        this.ont = ont;
    }

    public List<TransformerEntityType> getEntityTypes() {
        return entityTypes;
    }

    public void setEntityTypes(List<TransformerEntityType> entityTypes) {
        this.entityTypes = entityTypes;
    }

    public List<TransformerRelationType> getRelationTypes() {
        return relationTypes;
    }

    public void setRelationTypes(List<TransformerRelationType> relationTypes) {
        this.relationTypes = relationTypes;
    }
}

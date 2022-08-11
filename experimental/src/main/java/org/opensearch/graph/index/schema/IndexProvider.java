package org.opensearch.graph.index.schema;


import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.ImmutableList;
import org.opensearch.graph.ontology.Ontology;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "entities",
        "relations"
})
public class IndexProvider {

    @JsonProperty("ontology")
    private String ontology;
    @JsonProperty("entities")
    private List<Entity> entities = new ArrayList<>();
    @JsonProperty("relations")
    private List<Relation> relations = new ArrayList<>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();

    public IndexProvider() {}

    public IndexProvider(IndexProvider source) {
        this.ontology = source.ontology;
        this.additionalProperties.putAll(source.additionalProperties);
        this.entities.addAll(source.getEntities().stream().map(Entity::clone).collect(Collectors.toList()));
        this.relations.addAll(source.getRelations().stream().map(Relation::clone).collect(Collectors.toList()));
    }

    @JsonProperty("entities")
    public List<Entity> getEntities() {
        return Stream.concat(entities.stream()
                        .filter(e -> !e.getNested().isEmpty())
                        .flatMap(e -> e.getNested().stream()), entities.stream())
                .collect(Collectors.toList());
    }

    @JsonProperty("entities")
    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    @JsonProperty("relations")
    public List<Relation> getRelations() {
        return Stream.concat(relations.stream()
                        .filter(e -> !e.getNested().isEmpty())
                        .flatMap(e -> e.getNested().stream()), relations.stream())
                .collect(Collectors.toList());
    }

    @JsonProperty("relations")
    public void setRelations(List<Relation> relations) {
        this.relations = relations;
    }

    @JsonProperty("ontology")
    public String getOntology() {
        return ontology;
    }

    @JsonProperty("ontology")
    public void setOntology(String ontology) {
        this.ontology = ontology;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @JsonIgnore
    public IndexProvider withEntity(Entity entity) {
        entities.add(entity);
        return this;
    }

    @JsonIgnore
    public IndexProvider withRelation(Relation relation) {
        relations.add(relation);
        return this;
    }

    @JsonIgnore
    public Optional<Entity> getEntity(String label) {
        Optional<Entity> nest = getEntities().stream().filter(e -> !e.getNested().isEmpty())
                .flatMap(e -> e.getNested().stream())
                .filter(nested -> nested.getType().equals(label))
                .findAny();
        if (nest.isPresent())
            return nest;

        return getEntities().stream().filter(e -> e.getType().equals(label)).findAny();
    }

    @JsonIgnore
    public Optional<Relation> getRelation(String label) {
        Optional<Relation> nest = getRelations().stream().filter(e -> !e.getNested().isEmpty())
                .flatMap(e -> e.getNested().stream())
                .filter(nested -> nested.getType().equals(label))
                .findAny();
        if (nest.isPresent())
            return nest;

        return getRelations().stream().filter(e -> e.getType().equals(label)).findAny();
    }

    public static class Builder {

        /**
         * creates default index provider according to the given ontology - simple static index strategy
         *
         * @param ontology
         * @return
         */
        public static IndexProvider generate(Ontology ontology) {
            IndexProvider provider = new IndexProvider();
            provider.ontology = ontology.getOnt();
            //generate entities
            provider.entities = ontology.getEntityTypes().stream().map(e ->
                            new Entity(e.getName(), MappingIndexType.STATIC.name(), PartitionType.INDEX.name(),
                                    //E/S indices need to be lower cased
                                    new Props(ImmutableList.of(e.getName().toLowerCase())), Collections.emptyList(), Collections.emptyMap()))
                    .collect(Collectors.toList());
            //generate relations
            provider.relations = ontology.getRelationshipTypes().stream().map(e ->
                            new Relation(e.getName(), MappingIndexType.STATIC.name(), PartitionType.INDEX.name(), false, Collections.emptyList(),
                                    //E/S indices need to be lower cased
                                    new Props(ImmutableList.of(e.getName().toLowerCase())), Collections.emptyList(), Collections.emptyMap()))
                    .collect(Collectors.toList());

            return provider;
        }
    }
}

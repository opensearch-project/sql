package org.opensearch.graph.index.schema;


import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.ImmutableList;
import org.opensearch.graph.ontology.BaseElement;
import org.opensearch.graph.ontology.EntityType;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.RelationshipType;

import java.util.*;
import java.util.function.Predicate;
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

    public IndexProvider() {
    }

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

    @JsonProperty("rootEntities")
    public List<Entity> getRootEntities() {
        return entities;
    }

    @JsonProperty("rootEntities")
    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    @JsonProperty("rootRelations")
    public List<Relation> getRootRelations() {
        return relations;
    }

    @JsonProperty("relations")
    public List<Relation> getRelations() {
        return Stream.concat(relations.stream()
                        .filter(e -> !e.getNested().isEmpty())
                        .flatMap(e -> e.getNested().stream()), relations.stream())
                .collect(Collectors.toList());
    }

    @JsonProperty("rootRelations")
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

        public static IndexProvider generate(Ontology ontology) {
            return generate(ontology, e -> true, r -> true);
        }

        /**
         * creates default index provider according to the given ontology - simple static index strategy
         *
         * @param ontology
         * @return
         */
        public static IndexProvider generate(Ontology ontology, Predicate<EntityType> entityPredicate, Predicate<RelationshipType> relationPredicate) {
            Ontology.Accessor accessor = new Ontology.Accessor(ontology);
            IndexProvider provider = new IndexProvider();
            provider.ontology = ontology.getOnt();
            //generate entities
            provider.entities = ontology.getEntityTypes().stream()
                    .filter(entityPredicate)
                    .map(e -> createEntity(e,false, accessor))
                    .collect(Collectors.toList());
            //generate relations
            provider.relations = ontology.getRelationshipTypes().stream()
                    .filter(relationPredicate)
                    .map(e -> createRelation(e,false, accessor))
                    .collect(Collectors.toList());

            return provider;
        }

        private static Relation createRelation(RelationshipType r,boolean nested, Ontology.Accessor accessor) {
            return new Relation(r.getName(), nested ? MappingIndexType.NESTED.name() : MappingIndexType.STATIC.name(), PartitionType.INDEX.name(), false,
                    createNestedRelation(r, accessor),
                    // indices need to be lower cased
                    createProperties(r, accessor),
                    Collections.emptyList(), Collections.emptyMap());
        }

        private static Entity createEntity(EntityType e,boolean nested, Ontology.Accessor accessor) {
            return new Entity(e.getName(), nested ? MappingIndexType.NESTED.name() : MappingIndexType.STATIC.name(), PartitionType.INDEX.name(),
                    // indices need to be lower cased
                    createProperties(e, accessor),
                    createNestedEntity(e, accessor),
                    Collections.emptyMap());
        }

        private static List<Relation> createNestedRelation(RelationshipType r, Ontology.Accessor accessor) {
            return r.getProperties().stream()
                    .filter(p -> accessor.getNestedRelationByPropertyName(p).isPresent())
                    .map(p -> createRelation(accessor.getNestedRelationByPropertyName(p).get(),true, accessor))
                    .collect(Collectors.toList());
        }

        private static List<Entity> createNestedEntity(EntityType e, Ontology.Accessor accessor) {
            //filter nested entities only
            return e.getProperties().stream()
                    .filter(p -> accessor.getNestedEntityByPropertyName(p).isPresent())
                    .map(p -> createEntity(accessor.getNestedEntityByPropertyName(p).get(),true, accessor))
                    .collect(Collectors.toList());
        }

        private static <I extends BaseElement> Props createProperties(I e, Ontology.Accessor accessor) {
            return new Props(ImmutableList.of(e.getName()));
        }
    }
}

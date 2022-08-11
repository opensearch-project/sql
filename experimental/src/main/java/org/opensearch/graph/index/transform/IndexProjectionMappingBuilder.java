package org.opensearch.graph.index.transform;


import org.opensearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.opensearch.client.Client;
import org.opensearch.graph.index.template.PutIndexTemplateRequestBuilder;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.index.schema.Relation;
import org.opensearch.graph.index.template.TemplateMapping;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.Property;
import org.opensearch.graph.ontology.RelationshipType;

import java.util.*;

import static org.opensearch.graph.index.transform.OntologyIndexGenerator.EdgeSchemaConfig.DEST_ID;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.EdgeSchemaConfig.DEST_TYPE;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.IndexSchemaConfig.CHILD;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.IndexSchemaConfig.PROPERTIES;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.ProjectionConfigs.*;
import static org.opensearch.graph.index.transform.IndexMappingUtils.*;

public class IndexProjectionMappingBuilder {

    private IndexRelationsMappingBuilder relationsMappingBuilder;
    private IndexProvider indexProvider;

    public IndexProjectionMappingBuilder(IndexRelationsMappingBuilder relationsMappingBuilder, IndexProvider indexProvider) {
        this.relationsMappingBuilder = relationsMappingBuilder;
        this.indexProvider = indexProvider;
    }

    /**
     * wrap entities with projection related metadata fields for the purpose of the projection index mapping creation
     *
     * @param ontology
     * @return
     */
    Ontology.Accessor generateProjectionOntology(Ontology ontology) {
        //adding projection related metadata
        Ontology clone = new Ontology(ontology);
        //add projection related metadata
        clone.getEntityTypes().forEach(e -> e.withMetadata(Collections.singletonList("tag")));
        clone.getRelationshipTypes().forEach(r -> r.withMetadata(Collections.singletonList("tag")));
        clone.getRelationshipTypes().forEach(r -> r.withMetadata(Collections.singletonList(DEST_TYPE)));
        clone.getRelationshipTypes().forEach(r -> r.withMetadata(Collections.singletonList(DEST_ID)));

        clone.getProperties().add(new Property("tag", "tag", "string"));
        clone.getProperties().add(new Property(DEST_TYPE, DEST_TYPE, "string"));
        clone.getProperties().add(new Property(DEST_ID, DEST_ID, "string"));
        return new Ontology.Accessor(clone);
    }


    /**
     * add the mapping part of the template according to the ontology relations
     *
     * @return
     */


    /**
     * add the mapping part of the template according to the ontology
     * This projection mapping is a single unified index containing the entire ontology wrapped into a single index so that
     * every type of query result can be indexed and queried for slice & dice type of questions
     * <p>
     * "properties": {
     * "entityA": {
     * "type": "nested",
     * "properties": {
     * "entityA_id": {
     * "type": "integer",
     * },
     * "relationA": {
     * "type": "nested",
     * "properties": {
     * "relationA_id": {
     * "type": "integer",
     * }
     * }
     * }
     * }
     * },
     * "entityB": {
     * "type": "nested",
     * "properties": {
     * "entityB_id": {
     * "type": "integer",
     * },
     * "relationB": {
     * "type": "nested",
     * "properties": {
     * "relationB_id": {
     * "type": "integer",
     * }
     * }
     * }
     * }
     * }
     * }
     *
     * @param client
     * @return
     */
    public Collection<PutIndexTemplateRequestBuilder> map(Ontology.Accessor ontology, Client client, Map<String, PutIndexTemplateRequestBuilder> requests) {
        PutIndexTemplateRequestBuilder request = new PutIndexTemplateRequestBuilder(client, PutIndexTemplateAction.INSTANCE, "projection");
        request.setSettings(getDefaultSettings().build()).setPatterns(Collections.singletonList(String.format("%s*", PROJECTION)));

        Map<String, Object> jsonMap = new HashMap<>();
        Map<String, Object> rootMapping = new HashMap<>();
        Map<String, Object> rootProperties = new HashMap<>();
        rootMapping.put(PROPERTIES, rootProperties);

        //populate the query id
        rootProperties.put(QUERY_ID, parseType(ontology, "string"));
        rootProperties.put(CURSOR_ID, parseType(ontology, "string"));
        rootProperties.put(EXECUTION_TIME, parseType(ontology, "date"));
        //populate index fields
        jsonMap.put(PROJECTION, rootMapping);

        IndexProvider projection = new IndexProvider(this.indexProvider);
        //remove nested entities since we upgraded them to the root level
        projection.getEntities().forEach(e -> e.getNested().clear());

        projection.getEntities()
                .forEach(entity -> {
                    //todo remove nested entities since they already appear as a qualified ontological entity
                    try {
                        //generate entity mapping - each entity should be a nested objects array
                        Map<String, Object> objectMap = generateNestedEntityMapping(ontology, rootProperties, entity.withMapping(CHILD));
                        //generate relation mapping - each entity's relation should be a nested objects array inside the entity
                        List<RelationshipType> relationshipTypes = ontology.relationBySideA(entity.getType());
                        relationshipTypes.forEach(rel -> {
                            Relation relation = this.indexProvider.getRelation(rel.getName()).get();
                            relationsMappingBuilder.generateNestedRelationMapping(ontology, (Map<String, Object>) objectMap.get(PROPERTIES), relation.withMapping(CHILD));
                        });
                    } catch (Throwable typeNotFound) {
                        //log error
                    }
                });
        request.addMapping(PROJECTION, jsonMap);
        //add response to list of responses
        requests.put(PROJECTION, request);
        return requests.values();
    }
}
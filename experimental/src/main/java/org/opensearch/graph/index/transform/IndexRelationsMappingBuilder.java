package org.opensearch.graph.index.transform;

import org.opensearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.graph.GraphError;
import org.opensearch.graph.index.schema.BaseTypeElement;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.index.schema.MappingIndexType;
import org.opensearch.graph.index.schema.Relation;
import org.opensearch.graph.index.template.PutIndexTemplateRequestBuilder;
import org.opensearch.graph.index.template.SettingBuilder;
import org.opensearch.graph.index.template.TemplateMapping;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.RelationshipType;

import java.util.*;
import java.util.stream.Collectors;

import static org.opensearch.graph.index.transform.IndexMappingUtils.*;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.EdgeSchemaConfig.*;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.IndexSchemaConfig.*;

public class IndexRelationsMappingBuilder implements TemplateMapping<RelationshipType, Relation> {
    private IndexProvider indexProvider;

    public IndexRelationsMappingBuilder(IndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    public Collection<PutIndexTemplateRequestBuilder> map(Ontology.Accessor ontology, Client client, Map<String, PutIndexTemplateRequestBuilder> requests) {
        ontology.relations().forEach(r -> {
            String mapping = indexProvider.getRelation(r.getName()).orElseThrow(
                            () -> new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "No entity with name " + r + " found in ontology")))
                    .getPartition();

            Relation relation = indexProvider.getRelation(r.getName()).get();
            MappingIndexType type = MappingIndexType.valueOf(mapping.toUpperCase());
            switch (type) {
                case NESTED:
                    //this is implement in the populateNested() method
                    break;
                case UNIFIED:
                    //common general index - unifies all entities under the same physical index
                    relation.getProps().getValues().forEach(v -> {
                        String label = r.getrType();
                        String unifiedName = relation.getProps().getValues().isEmpty() ? label : relation.getProps().getValues().get(0);
                        PutIndexTemplateRequestBuilder request = requests.computeIfAbsent(unifiedName, s -> new PutIndexTemplateRequestBuilder(client, PutIndexTemplateAction.INSTANCE, unifiedName));

                        List<String> patterns = new ArrayList<>(Arrays.asList(r.getName().toLowerCase(), label, r.getName(), String.format("%s%s", v, "*")));
                        if (Objects.isNull(request.request().patterns())) {
                            request.setPatterns(new ArrayList<>(patterns));
                        } else {
                            request.request().patterns().addAll(patterns);
                        }
                        //dedup patterns
                        request.setPatterns(request.request().patterns().stream().distinct().collect(Collectors.toList()));

                        //no specific index sort order since it contains multiple entity types -
                        if (request.request().settings().isEmpty()) {
                            request.setSettings(getDefaultSettings().build());
                        }
                        //create new mapping only when no prior entity set this mapping before
                        if (request.request().mappings().isEmpty()) {
                            request.addMapping(unifiedName, generateElementMapping(ontology, r, relation, unifiedName));
                        } else {
                            populateProperty(ontology, relation, request.getMappingsProperties(unifiedName), r);
                        }
                    });
                    break;
                case STATIC:
                    //static index
                    relation.getProps().getValues().forEach(v -> {
                        String label = r.getrType();
                        PutIndexTemplateRequestBuilder request = new PutIndexTemplateRequestBuilder(client, PutIndexTemplateAction.INSTANCE, label.toLowerCase());
                        request.setPatterns(new ArrayList<>(Arrays.asList(r.getName().toLowerCase(), label, r.getName(), String.format("%s%s", v, "*"))))
                                .setSettings(generateSettings(ontology, r, relation, label))
                                .addMapping(label, generateElementMapping(ontology, r, relation, label));
                        //dedup patterns -
                        request.setPatterns(request.request().patterns().stream().distinct().collect(Collectors.toList()));
                        //add response to list of responses
                        requests.put(label.toLowerCase(), request);
                    });
                    break;
                case TIME:
                    String label = r.getrType();
                    PutIndexTemplateRequestBuilder request = new PutIndexTemplateRequestBuilder(client, PutIndexTemplateAction.INSTANCE, relation.getType().toLowerCase());
                    //todo - Only the time based partition will have a template suffix with astrix added to allow numbering and dates as part of the naming convention
                    request.setPatterns(new ArrayList<>(Arrays.asList(r.getName().toLowerCase(), label, r.getName(), String.format(relation.getProps().getIndexFormat(), "*"))))
                            .setSettings(generateSettings(ontology, r, relation, label))
                            .addMapping(label, generateElementMapping(ontology, r, relation, label));
                    //add response to list of responses

                    //dedup patterns -
                    request.setPatterns(request.request().patterns().stream().distinct().collect(Collectors.toList()));

                    //add the request
                    requests.put(relation.getType(), request);
                    break;
                default:
                    String result = "No mapping found";
                    break;
            }
        });
        return requests.values();
    }

    /**
     * generate specific relation type mapping
     *
     * @param relationshipType
     * @param label
     * @return
     */
    public Map<String, Object> generateElementMapping(Ontology.Accessor ontology, RelationshipType relationshipType, Relation rel, String label) {
        Optional<RelationshipType> relation = ontology.relation(relationshipType.getName());
        if (!relation.isPresent())
            throw new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "No relation    with name " + label + " found in ontology"));

        Map<String, Object> jsonMap = new HashMap<>();

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put(PROPERTIES, properties);

        //generate field id -> only if field id array size > 1
        if (relationshipType.getIdField().size() > 1) {
            properties.put(relationshipType.idFieldName(), Collections.singletonMap("type", "keyword"));
        }//otherwise that field id is already a part of the regular fields


        //populate fields & metadata
        relation.get().getMetadata().forEach(v -> properties.put(v, parseType(ontology, ontology.property$(v).getType())));
        relation.get().getProperties().forEach(v -> properties.put(v, parseType(ontology, ontology.property$(v).getType())));
        //set direction
        properties.put(DIRECTION, parseType(ontology, "string"));
        //populate  sideA (entityA)
        populateRedundant(ontology, SOURCE, relationshipType.getName(), properties);
        //populate  sideB (entityB)
        populateRedundant(ontology, DEST, relationshipType.getName(), properties);
        //populate nested documents
        rel.getNested().forEach(nest -> generateNestedRelationMapping(ontology, properties, nest));

        //add mapping only if properties size > 0
        if (properties.size() > 0) {
            jsonMap.put(label, mapping);
        }
        return jsonMap;
    }

    void generateNestedRelationMapping(Ontology.Accessor ontology, Map<String, Object> parent, BaseTypeElement<? extends BaseTypeElement> nest) {
        Optional<RelationshipType> relation = ontology.relation(nest.getType());
        if (!relation.isPresent())
            throw new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "No relation with name " + nest.getType() + " found in ontology"));

        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        switch (nest.getMapping()) {
            case EMBEDDED:
                //no specific mapping here -
                break;
            case CHILD:
                mapping.put(TYPE, NESTED);
                break;
        }
        mapping.put(PROPERTIES, properties);
        //populate fields & metadata
        populateProperty(ontology, nest, properties, relation.get());
        //assuming single value exists (this is the field name)
        if (nest.getProps().getValues().isEmpty())
            throw new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "Nested Rel with name " + nest.getType() + " has no property value in mapping file"));

        //inner child nested population
        nest.getNested().forEach(inner -> generateNestedRelationMapping(ontology, properties, inner));
        //assuming single value exists (this is the field name)
        //add mapping only if properties size > 0
        if (properties.size() > 0) {
            parent.put(nest.getType(), mapping);
        }
    }

    private void populateRedundant(Ontology.Accessor ontology, String side, String label, Map<String, Object> properties) {
        HashMap<String, Object> sideProperties = new HashMap<>();
        properties.put(side, sideProperties);
        HashMap<String, Object> values = new HashMap<>();
        sideProperties.put(PROPERTIES, values);

        //add side ID
        values.put(ID, parseType(ontology, ontology.property$(ID).getType()));
        //add side TYPE
        values.put(TYPE, parseType(ontology, ontology.property$(TYPE).getType()));
        indexProvider.getRelation(label).get().getRedundant(side)
                .forEach(r -> values.put(r.getName(), parseType(ontology, ontology.property$(r.getName()).getType())));
    }

    /**
     * add the index relation settings part of the template according to the ontology relations
     *
     * @return
     */
    private Settings generateSettings(Ontology.Accessor ontology, RelationshipType relationType, Relation rel, String label) {
        ontology.relation(relationType.getName()).get().getIdField().forEach(idField -> {
            if (!ontology.relation(relationType.getName()).get().fields().contains(idField))
                throw new GraphError.GraphErrorException(new GraphError("Relation Schema generation exception", " Relationship " + label + " not containing id metadata property "));
        });
        return builder(ontology, rel);
    }

    private Settings builder(Ontology.Accessor ontology, Relation relation) {
        SettingBuilder settings = getDefaultSettings();
        if (relation.getNested().isEmpty()) {
            //assuming id is a mandatory part of metadata/properties
            settings.sortByField(ontology.relation$(relation.getType()).idFieldName(), true);
        }
        return settings.build();
    }
}

package org.opensearch.graph.index.transform;

import javaslang.Tuple2;
import org.opensearch.graph.GraphError;
import org.opensearch.graph.index.schema.BaseTypeElement;
import org.opensearch.graph.index.template.SettingBuilder;
import org.opensearch.graph.ontology.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.EdgeSchemaConfig.DEST_ID;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.EdgeSchemaConfig.DEST_TYPE;
import static org.opensearch.graph.index.transform.OntologyIndexGenerator.IndexSchemaConfig.*;

public abstract class IndexMappingUtils {

    static SettingBuilder getDefaultSettings() {
        return SettingBuilder.create().shards(3).replicas(1);
    }
    /**
     * parse ontology primitive type to opensearch primitive type
     *
     * @param nameType
     * @return
     */
    public static Map<String, Object> parseType(Ontology.Accessor ontology, PropertyType nameType) {
        Map<String, Object> map = new HashMap<>();
        try {
            Ontology.OntologyPrimitiveType type = Ontology.OntologyPrimitiveType.valueOf(nameType.getType());
            switch (type) {
                case STRING:
                    map.put("type", "keyword");
                    break;
                case TEXT:
                    map.put("type", "text");
                    map.put("fields", singletonMap("keyword", singletonMap("type", "keyword")));
                    break;
                case DATE:
                    map.put("type", "date");
                    map.put("format", "epoch_millis||strict_date_optional_time||yyyy-MM-dd HH:mm:ss.SSS");
                    break;
                case LONG:
                    map.put("type", "long");
                    break;
                case INT:
                    map.put("type", "integer");
                    break;
                case FLOAT:
                    map.put("type", "float");
                    break;
                case DOUBLE:
                    map.put("type", "double");
                    break;
                case GEO:
                    map.put("type", "geo_point");
                    break;
            }
        } catch (Throwable typeNotFound) {
            // manage non-primitive type such as enum or nested typed
            Optional<Tuple2<Ontology.Accessor.NodeType, String>> type = ontology.matchNameToType(nameType.getType());
            if (type.isPresent()) {
                switch (type.get()._1()) {
                    case ENTITY:
                        //todo - manage the nested-embedded type here
                        break;
                    case ENUM:
                        //enum is always backed by integer
                        map.put("type", "integer");
                        break;
                    case RELATION:
                        break;
                }
            } else {
                //default
                map.put("type", "text");
                map.put("fields", singletonMap("keyword", singletonMap("type", "keyword")));
            }
        }
        return map;
    }

    static void populateProperty(Ontology.Accessor ontology, BaseTypeElement<? extends BaseTypeElement> element, Map<String, Object> properties, BaseElement entityType) {
        entityType.getMetadata().forEach(v -> {
            Map<String, Object> parseType = parseType(ontology, ontology.property$(v).getType());
            if (!parseType.isEmpty()) properties.put(v, parseType);
        });
        entityType.getProperties().forEach(v -> {
            Map<String, Object> parseType = parseType(ontology, ontology.property$(v).getType());
            if (!parseType.isEmpty()) properties.put(v, parseType);
        });
        //populate nested documents
        populateNested(ontology, element, properties);
    }

    static void populateNested(Ontology.Accessor ontology, BaseTypeElement<? extends BaseTypeElement> element, Map<String, Object> properties) {
        element.getNested().forEach(nest -> generateNestedEntityMapping(ontology, properties, nest));
    }

    static Map<String, Object> generateNestedEntityMapping(Ontology.Accessor ontology, Map<String, Object> parent, BaseTypeElement<? extends BaseTypeElement> nest) {
        Optional<EntityType> entity = ontology.entity(nest.getType());
        if (!entity.isPresent())
            throw new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "No entity with name " + nest.getType() + " found in ontology"));

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
        populateProperty(ontology, nest, properties, entity.get());
        //assuming single value exists (this is the field name)
        if (nest.getProps().getValues().isEmpty())
            throw new GraphError.GraphErrorException(new GraphError("Mapping generation exception", "Nested entity with name " + nest.getType() + " has no property value in mapping file"));

        //add mapping only if properties size > 0
        if (properties.size() > 0) {
            parent.put(nest.getType(), mapping);
        }
        return mapping;
    }


}

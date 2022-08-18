package org.opensearch.graph.ontology;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "pType")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "Primitive", value = PrimitiveType.class),
        @JsonSubTypes.Type(name = "PrimitiveList", value = PrimitiveType.ArrayOfPrimitives.class),
        @JsonSubTypes.Type(name = "Object", value = ObjectType.class),
        @JsonSubTypes.Type(name = "ObjectList", value = ObjectType.ArrayOfObjects.class)
})
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface PropertyType {
    String getType();
}

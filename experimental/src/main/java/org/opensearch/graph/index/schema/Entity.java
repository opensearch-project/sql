package org.opensearch.graph.index.schema;


import com.fasterxml.jackson.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "type",
        "partition",
        "props",
        "nested"
})
public class Entity implements BaseTypeElement<Entity> {

    @JsonProperty("type")
    private String type;
    @JsonProperty("partition")
    private String partition;
    @JsonProperty("mapping")
    private String mapping;
    @JsonProperty("props")
    private Props props;
    @JsonProperty("nested")
    private List<Entity> nested = Collections.EMPTY_LIST;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();

    public Entity() {}

    public Entity(String type, String partition, String mapping, Props props, List<Entity> nested, Map<String, Object> additionalProperties) {
        this.type = type;
        this.partition = partition;
        this.mapping = mapping;
        this.props = props;
        this.nested = nested;
        this.additionalProperties = additionalProperties;
    }

    @JsonProperty("nested")
    public List<Entity> getNested() {
        return nested;
    }

    @JsonProperty("nested")
    public void setNested(List<Entity> nested) {
        this.nested = nested;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("partition")
    public String getPartition() {
        return partition;
    }

    @JsonProperty("mapping")
    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    @JsonProperty("mapping")
    public String getMapping() {
        return mapping;
    }

    @JsonProperty("partition")
    public void setPartition(String partition) {
        this.partition = partition;
    }

    @JsonProperty("props")
    public Props getProps() {
        return props;
    }

    @JsonProperty("props")
    public void setProps(Props props) {
        this.props = props;
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
    public Entity withMapping(String mapping) {
        this.mapping = mapping;
        return this;
    }

    @Override
    protected Entity clone()  {
        return new Entity(this.type,this.partition,this.mapping,this.props.clone(),
                this.nested.stream().map(Entity::clone).collect(Collectors.toList()),
                new HashMap<>(this.additionalProperties));
    }

    @JsonIgnore
    public Entity withType(String type) {
        this.type = type;
        return this;
    }

    @JsonIgnore
    public Entity withPartition(String partition) {
        this.partition = partition;
        return this;
    }
}

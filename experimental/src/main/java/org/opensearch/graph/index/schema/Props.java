package org.opensearch.graph.index.schema;





import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "values"
})
public class Props {


    @JsonProperty("values")
    private List<String> values = null;
    @JsonProperty("partition.field")
    private String partitionField;
    @JsonProperty("prefix")
    private String prefix;
    @JsonProperty("index.format")
    private String indexFormat;
    @JsonProperty("date.format")
    private String dateFormat;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public Props() {}

    public Props(List<String> values) {
        this.values = values;
    }

    public Props(List<String> values, String partitionField, String prefix, String indexFormat, String dateFormat, Map<String, Object> additionalProperties) {
        this.values = values;
        this.partitionField = partitionField;
        this.prefix = prefix;
        this.indexFormat = indexFormat;
        this.dateFormat = dateFormat;
        this.additionalProperties = additionalProperties;
    }

    @JsonProperty("values")
    public List<String> getValues() {
        return values;
    }

    @JsonProperty("values")
    public void setValues(List<String> values) {
        this.values = values;
    }

    @JsonProperty("partition.field")
    public String getPartitionField() {
        return partitionField;
    }

    @JsonProperty("partition.field")
    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

    @JsonProperty("prefix")
    public String getPrefix() {
        return prefix;
    }

    @JsonProperty("prefix")
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @JsonProperty("index.format")
    public String getIndexFormat() {
        return indexFormat;
    }

    @JsonProperty("index.format")
    public void setIndexFormat(String indexFormat) {
        this.indexFormat = indexFormat;
    }

    @JsonProperty("date.format")
    public String getDateFormat() {
        return dateFormat;
    }

    @JsonProperty("date.format")
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    protected Props clone()  {
        return new Props(this.values);
    }
}

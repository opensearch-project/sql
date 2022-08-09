package org.opensearch.graph.ontology.transformer;






import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TransformerProperties {
    private String pattern;
    private String label;
    private String concreteType;
    private List<Map<String,String>> valuePatterns;

    public TransformerProperties() {}

    public TransformerProperties(String pattern,String label, String eType, List<Map<String, String>> valuePatterns) {
        this.label = label;
        this.pattern = pattern;
        this.concreteType = eType;
        this.valuePatterns = valuePatterns;
    }

    public String getLabel() {
        return label;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getConcreteType() {
        return concreteType;
    }

    public void setConcreteType(String concreteType) {
        this.concreteType = concreteType;
    }

    public List<Map<String,String>> getValuePatterns() {
        return valuePatterns;
    }

    public void setValuePatterns(List<Map<String,String>> valuePatterns) {
        this.valuePatterns = valuePatterns;
    }
}

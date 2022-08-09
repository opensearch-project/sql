package org.opensearch.graph.ontology;








import java.util.List;

public class CompositeType {

    public String getcType() {
        return cType;
    }

    public void setcType(String cType) {
        this.cType = cType;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }

    //region Fields
    private String cType;
    private List<Property> properties;
    //endregion
}

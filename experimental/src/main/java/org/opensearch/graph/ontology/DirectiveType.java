package org.opensearch.graph.ontology;







public class DirectiveType {
    private DirectiveClasses type;

    private String domain;
    private String range;
    private String name;

    public DirectiveType(String name, DirectiveClasses type, String domain, String range) {
        this.type = type;
        this.domain = domain;
        this.range = range;
        this.name = name;
    }

    public DirectiveClasses getType() {
        return type;
    }

    public void setType(DirectiveClasses type) {
        this.type = type;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public enum DirectiveClasses {
        PROPERTY, CLASS, DATATYPE, RESOURCE, LITERAL
    }
}

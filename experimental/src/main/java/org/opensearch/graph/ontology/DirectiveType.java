package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DirectiveType {
    private DirectiveClasses type;

    private String domain;
    private String range;
    private String name;

    private List<Argument> arguments;
    public DirectiveType() {}
    public DirectiveType(String name, DirectiveClasses type, String domain, String range) {
        this.type = type;
        this.domain = domain;
        this.range = range;
        this.name = name;
        this.arguments = new ArrayList<>();
    }

   public DirectiveType(String name, DirectiveClasses type,List<Argument> arguments) {
        this.type = type;
        this.name = name;
        this.arguments = arguments;
    }

    public DirectiveClasses getType() {
        return type;
    }

    public void setType(DirectiveClasses type) {
        this.type = type;
    }

    public List<Argument> getArguments() {
        return arguments;
    }

    public void setArguments(List<Argument> arguments) {
        this.arguments = arguments;
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

    public boolean containsArgVal(Object value) {
        return getArguments().stream().anyMatch(arg->arg.value.toString().equals(value));
    }
    public boolean containsArg(String name) {
        return getArguments().stream().anyMatch(arg->arg.name.equals(name));
    }
    public enum DirectiveClasses {
        PROPERTY, CLASS, DATATYPE, RESOURCE, LITERAL
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DirectiveType that = (DirectiveType) o;
        return type == that.type && Objects.equals(domain, that.domain) && Objects.equals(range, that.range) && Objects.equals(name, that.name) && Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, domain, range, name, arguments);
    }

    /**
     * directives argument
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Argument {
        public static Argument of(String name, Object value) {
            return new Argument(name, value);
        }

        private Argument() {}
        private Argument(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String name;
        public Object value;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Argument argument = (Argument) o;
            return Objects.equals(name, argument.name) && Objects.equals(value, argument.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }
    }
}

package org.opensearch.graph.ontology;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * Epair represents the connection between two entities in the ontology
 * - TypeA states the side-A entity type
 * - sideAIdField states the side-A related (FK) field name as it appears in the connecting table
 * (the actual field name on the Side-A entity if stated according to that entity's own fieldID - PK )
 * - TypeB states the side-B entity type
 * - sideBIdField states the side-B related(FK)  field name as it appears in the connecting table
 * (the actual field name on the Side-B entity if stated according to that entity's own fieldID - PK )
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EPair {
    /**
     * DO-NOT-REMOVE - @Jackson required
     */
    public EPair() {}

    public EPair(String eTypeA, String eTypeB) {
        this(String.format("%s->%s", eTypeA, eTypeB), eTypeA, eTypeB);
    }


    public EPair(String name, String eTypeA, String eTypeB) {
        this.eTypeA = eTypeA;
        this.eTypeB = eTypeB;
        this.name = name;
    }

    public EPair(String eTypeA, String sideAFieldName, String sideAIdField, String eTypeB, String sideBIdField) {
        this(String.format("%s->%s", eTypeA, eTypeB), eTypeA, sideAFieldName, sideAIdField, eTypeB, sideBIdField);
    }

    public EPair(String name, String eTypeA, String sideAFieldName, String sideAIdField, String eTypeB, String sideBIdField) {
        this.name = name;
        this.eTypeA = eTypeA;
        this.sideAFieldName = sideAFieldName;
        this.sideAIdField = sideAIdField;
        this.eTypeB = eTypeB;
        this.sideBIdField = sideBIdField;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String geteTypeA() {
        return eTypeA;
    }

    public void seteTypeA(String eTypeA) {
        this.eTypeA = eTypeA;
    }

    public String geteTypeB() {
        return eTypeB;
    }

    public void seteTypeB(String eTypeB) {
        this.eTypeB = eTypeB;
    }

    public String getSideAIdField() {
        return sideAIdField;
    }

    public void setSideAIdField(String sideAIdField) {
        this.sideAIdField = sideAIdField;
    }

    public String getSideBIdField() {
        return sideBIdField;
    }

    public void setSideBIdField(String sideBIdField) {
        this.sideBIdField = sideBIdField;
    }

    public String getSideAFieldName() {
        return sideAFieldName;
    }

    public void setSideAFieldName(String sideAFieldName) {
        this.sideAFieldName = sideAFieldName;
    }

    @JsonIgnore
    public EPair withSideAIdField(String sideAIdField) {
        this.sideAIdField = sideAIdField;
        return this;
    }

    @JsonIgnore
    public EPair withSideBIdField(String sideBIdField) {
        this.sideBIdField = sideBIdField;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EPair ePair = (EPair) o;
        return
                Objects.equals(name, ePair.name) &&
                        Objects.equals(eTypeA, ePair.eTypeA) &&
                        Objects.equals(sideAFieldName, ePair.sideAFieldName) &
                                Objects.equals(sideAIdField, ePair.sideAIdField) &
                                Objects.equals(eTypeB, ePair.eTypeB) &
                                Objects.equals(sideBIdField, ePair.sideBIdField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, eTypeA, sideAFieldName, sideAIdField, eTypeB, sideBIdField);
    }

    @Override
    public String toString() {
        return "EPair [name= " + name + ",eTypeA= " + eTypeA + ",sideAId= " + sideAIdField + ",sideAField= " + sideAFieldName + ", eTypeB = " + eTypeB + ", sideAId = " + sideBIdField + "]";
    }

    @Override
    public EPair clone() {
        return new EPair(name, eTypeA, sideAFieldName, sideAIdField, eTypeB, sideBIdField);
    }

    //region Fields
    private String name;
    private String eTypeA;
    private String sideAFieldName = "field_name";
    private String sideAIdField = "source_id";
    private String eTypeB;
    private String sideBIdField = "dest_id";

    //endregion

}

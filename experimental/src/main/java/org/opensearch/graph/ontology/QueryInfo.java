package org.opensearch.graph.ontology;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class QueryInfo<Query> {
    private Query query;
    private String queryName;
    private String queryType;
    private String ontology;

    /**
     *
     * @param query - the actual quary
     * @param queryName - name
     * @param queryType - the query language type ( v1,cypher,sparql,graphql)
     * @param ontology - ontology name
     */
    public QueryInfo(Query query, String queryName, String queryType, String ontology) {
        this.query = query;
        this.queryName = queryName;
        this.queryType = queryType;
        this.ontology = ontology;
    }

    /**
     *  the query language type ( v1,cypher,sparql,graphql)
     * @return
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * query name
     * @return
     */
    public String getQueryName() {
        return queryName;
    }

    public Query getQuery() {
        return query;
    }

    public String getOntology() {
        return ontology;
    }
}

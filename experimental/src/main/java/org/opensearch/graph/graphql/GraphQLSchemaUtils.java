package org.opensearch.graph.graphql;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.TypeDefinitionRegistry;

public interface GraphQLSchemaUtils {
    GraphQLSchema getGraphQLSchema();

    TypeDefinitionRegistry getTypeRegistry();
}

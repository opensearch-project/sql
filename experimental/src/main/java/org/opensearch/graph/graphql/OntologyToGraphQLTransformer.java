package org.opensearch.graph.graphql;

import graphql.language.*;
import graphql.schema.*;
import org.opensearch.graph.ontology.EntityType;
import org.opensearch.graph.ontology.EnumeratedType;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.RelationshipType;

import java.util.List;
import java.util.stream.Collectors;

import static graphql.Scalars.*;
import static graphql.schema.GraphQLSchema.newSchema;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static org.opensearch.graph.graphql.GraphQLSchemaUtils.QUERY;
import static org.opensearch.graph.graphql.GraphQLSchemaUtils.WhereSupportGraphQL.*;

/**
 *
 */
public class OntologyToGraphQLTransformer {
    //graph QL reader and schema parts
    private GraphQLSchema graphQLSchema;

    /**
     * schema query builder for each entity
     *
     * @param accessor
     * @return
     */
    private GraphQLObjectType buildQuery(Ontology.Accessor accessor) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
                .definition(ObjectTypeDefinition.newObjectTypeDefinition()
                        .name(QUERY)
                        .build())
                .name(QUERY);

        //build input query with where clause for each entity
        accessor.entities()
                .forEach(e ->
                        builder.field(
                                GraphQLFieldDefinition.newFieldDefinition()
                                        .name(e.geteType())
                                        .argument(new GraphQLArgument.Builder()
                                                .name(WHERE_CLAUSE)
                                                .type(typeRef(WHERE_CLAUSE))
                                                .build())
                                        .type(typeRef(e.geteType()))
                                        .definition(new FieldDefinition(e.geteType(), new TypeName(e.geteType())))
                                        .build()));

        return builder.build();
    }

    /**
     * API that will transform a Ontology schema into grpahQL schema
     *
     * @param source
     * @return
     */
    public GraphQLSchema transform(Ontology source) {
        if (graphQLSchema == null) {
            //create schema
            GraphQLSchema.Builder builder = newSchema();
            // each registry is merged into the main registry
            Ontology.Accessor accessor = new Ontology.Accessor(source);
            accessor.getEnumeratedTypes().forEach(t -> builder.additionalType(buildEnum(t, accessor)));
            accessor.entities().forEach(e -> builder.additionalType(buildGraphQLObject(e, accessor)));
            //add where input type
            buildWhereInputType(builder);
            // add query for all concrete types with the where input type
            builder.query(buildQuery(accessor));
            graphQLSchema = builder.build();
        } else {
            Ontology.Accessor accessor = new Ontology.Accessor(source);
            accessor.getEnumeratedTypes().forEach(t -> graphQLSchema.getAllTypesAsList().add(buildEnum(t, accessor)));
            accessor.entities().forEach(e -> graphQLSchema.getAllTypesAsList().add(buildGraphQLObject(e, accessor)));
        }
        return graphQLSchema;

    }

    private GraphQLEnumType buildEnum(EnumeratedType type, Ontology.Accessor accessor) {
        return new GraphQLEnumType.Builder()
                .name(type.geteType())
                .values(type.getValues().stream()
                        .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition().name(v.getName()).value(v.getVal()).build())
                        .collect(Collectors.toList()))
                //build definition
                .definition(EnumTypeDefinition.newEnumTypeDefinition()
                        .name(type.geteType())
                        .enumValueDefinitions(type.getValues().stream()
                                .map(v -> new EnumValueDefinition(v.getName()))
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }


    private GraphQLObjectType buildGraphQLObject(EntityType e, Ontology.Accessor accessor) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
                .name(e.geteType());

        e.getProperties().forEach(p -> builder.field(
                GraphQLFieldDefinition.newFieldDefinition()
                        .name(p)
                        .type(type(accessor.property$(p).getType().getType(), accessor))
                        .definition(new FieldDefinition(p, new TypeName(accessor.property$(p).getType().getType())))
                        .build()
        ));

        List<RelationshipType> relationshipTypes = accessor.relationBySideA(e.geteType());
        relationshipTypes.forEach(rel -> builder.field(
                        GraphQLFieldDefinition.newFieldDefinition()
                                .name(rel.getName())
                                .definition(new FieldDefinition(rel.getName(), new ListType(new TypeName(rel.getePairs().get(0).geteTypeB()))))
                                //all pairs should follow same type pattern (todo check is this always correct)
                                .type(GraphQLList.list(typeRef(rel.getePairs().get(0).geteTypeB()))))
                .build());

        //definitions
        ObjectTypeDefinition.Builder defBuilder = ObjectTypeDefinition.newObjectTypeDefinition();
        //fields definitions
        e.getProperties().forEach(p -> defBuilder.fieldDefinition(
                FieldDefinition.newFieldDefinition()
                        .name(p)
                        .type(TypeName.newTypeName(p).build())
                        .build()
        ));
        //additional entities created from relationships (graphQL considers them as embedded relations)
        relationshipTypes.forEach(rel -> defBuilder.fieldDefinition(
                FieldDefinition.newFieldDefinition()
                        .name(rel.getName())
                        //all pairs should follow same type pattern (todo check is this always correct)
                        .type(ListType.newListType(
                                        TypeName.newTypeName(rel.getePairs().get(0).geteTypeB()).build())
                                .build())
                        .build()));

        builder.definition(defBuilder.build());
        return builder.build();
    }

    private GraphQLOutputType type(String type, Ontology.Accessor accessor) {
        if (accessor.enumeratedType(type).isPresent())
            return typeRef(type);

        switch (type) {
            case "ID":
                return GraphQLID;
            case "String":
                return GraphQLString;
            case "Int":
                return GraphQLInt;
            default:
                return GraphQLString;
        }

    }

}

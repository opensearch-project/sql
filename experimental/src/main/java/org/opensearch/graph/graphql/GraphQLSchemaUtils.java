package org.opensearch.graph.graphql;

import graphql.language.*;
import graphql.schema.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import javaslang.Tuple2;
import org.opensearch.graph.ontology.DirectiveType;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static org.opensearch.graph.ontology.DirectiveType.Argument.of;

public interface GraphQLSchemaUtils {
    String QUERY = "Query";

    GraphQLSchema getGraphQLSchema();

    TypeDefinitionRegistry getTypeRegistry();

    static Optional<String> getIDFieldName(GraphQLObjectType object) {
        return getFieldByType(object, new FieldDefinition("id", TypeName.newTypeName(GraphQLID.getName()).build()))
                .map(GraphQLFieldDefinition::getName);
    }

    static Optional<GraphQLFieldDefinition> getFieldByType(GraphQLObjectType object, FieldDefinition definition) {
        return object.getFieldDefinitions().stream()
                .filter(p -> p.getDefinition().isEqualTo(definition)).findAny();
    }

    static Optional<GraphQLFieldDefinition> getFieldByType(List<GraphQLFieldDefinition> fieldDefinitions, String type) {
        return fieldDefinitions.stream()
                .filter(p -> p.getDefinition().getName().equals(type))
                .findAny();
    }

    static Collection<DirectiveType> formatDirective(Optional<? extends GraphQLDirectiveContainer> element) {
        return (element.isEmpty() ? Collections.emptyList() :
                element.get().getAppliedDirectives().stream()
                        .map(d -> new DirectiveType(d.getName(),
                                DirectiveType.DirectiveClasses.DATATYPE,
                                d.getArguments().stream()
                                        .map(arg -> of(arg.getName(), arg.getValue()))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList()));
    }

    /**
     * filter entity type according to predicate
     *
     * @param type
     * @param predicate
     * @return
     */
    static Optional<Tuple2<String, TypeName>> filter(Type type, String field, Predicate<TypeName> predicate) {
        //scalar type property
        if ((type instanceof TypeName) && (predicate.test((TypeName) type)))
            return Optional.of(new Tuple2(field, type));

        //list type
        if (type instanceof ListType) {
            return filter(((ListType) type).getType(), field, predicate);
        }
        //non null type - may contain all sub-types (wrapper)
        if (type instanceof NonNullType) {
            Type rawType = ((NonNullType) type).getType();

            //validate only scalars are registered as properties
            if ((rawType instanceof TypeName) && predicate.test((TypeName) rawType)) {
                return Optional.of(new Tuple2(field, rawType));
            }

            if (rawType instanceof ListType) {
                return filter(((ListType) rawType).getType(), field, predicate);
            }
        }

        return Optional.empty();
    }

    /**
     * supporting where clause semantics
     */
    class WhereSupportGraphQL {
        public static final String WHERE_OPERATOR = "WhereOperator";
        public static final String WHERE_CLAUSE = "WhereClause";
        public static final String OR = "OR";
        public static final String AND = "AND";
        public static final String CONSTRAINT = "Constraint";
        public static final String OPERAND = "operand";
        public static final String OPERATOR = "operator";
        public static final String EXPRESSION = "expression";

        public static void buildWhereInputType(GraphQLSchema.Builder builder) {
            //where enum
            builder.additionalType(new GraphQLEnumType.Builder()
                    .name(WHERE_OPERATOR)
                    .values(Arrays.asList(GraphQLEnumValueDefinition.newEnumValueDefinition().name(OR).value(OR).build(),
                            GraphQLEnumValueDefinition.newEnumValueDefinition().name(AND).value(AND).build()))
                    //definition
                    .definition(EnumTypeDefinition.newEnumTypeDefinition()
                            .name(WHERE_OPERATOR)
                            .enumValueDefinitions(Arrays.asList(new EnumValueDefinition(OR),
                                    new EnumValueDefinition(AND)))
                            .build())
                    .build());

            //Constraint
            /**
             *     input Constraint {
             *         operand: String!
             *         operator: String!
             *         expression: String
             *     }
             */
            builder.additionalType(GraphQLInputObjectType.newInputObject()
                    .name(CONSTRAINT)
                    .field(GraphQLInputObjectField.newInputObjectField()
                            .name(OPERAND)
                            .type(new GraphQLNonNull(GraphQLID))
                            .build())
                    .field(GraphQLInputObjectField.newInputObjectField()
                            .name(OPERATOR)
                            .type(new GraphQLNonNull(GraphQLString))
                            .build())
                    .field(GraphQLInputObjectField.newInputObjectField()
                            .name(EXPRESSION)
                            .type(GraphQLString)
                            .build())
                    //definition
                    .definition(InputObjectTypeDefinition.newInputObjectDefinition()
                            .name(CONSTRAINT)
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name(OPERAND)
                                    .type(NonNullType.newNonNullType()
                                            .type(TypeName.newTypeName(GraphQLID.getName())
                                                    .build())
                                            .build())
                                    .build())
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name(OPERATOR)
                                    .type(NonNullType.newNonNullType()
                                            .type(TypeName.newTypeName(GraphQLString.getName())
                                                    .build())
                                            .build())
                                    .build())
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name(EXPRESSION)
                                    .type(TypeName.newTypeName(GraphQLString.getName())
                                            .build())
                                    .build())
                            .build())
                    .build());

            //where clause
            /**
             *     input WhereClause {
             *         operator: WhereOperator
             *         constraints: [Constraint]
             *     }
             */

            builder.additionalType(GraphQLInputObjectType.newInputObject()
                    .name(WHERE_CLAUSE)
                    .field(GraphQLInputObjectField.newInputObjectField()
                            .name(OPERATOR)
                            .type(typeRef(WHERE_OPERATOR)))
                    .field(GraphQLInputObjectField.newInputObjectField()
                            .name(CONSTRAINT)
                            .type(GraphQLList.list(typeRef(CONSTRAINT))))


                    //definition
                    .definition(InputObjectTypeDefinition.newInputObjectDefinition()
                            .name(WHERE_CLAUSE)
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name(OPERATOR)
                                    .type(TypeName.newTypeName(WHERE_OPERATOR)
                                            .build())
                                    .build())
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name(CONSTRAINT)
                                    .type(ListType.newListType(
                                                    TypeName.newTypeName(CONSTRAINT)
                                                            .build())
                                            .build())
                                    .build())
                            .build())
                    .build());
        }
    }

}

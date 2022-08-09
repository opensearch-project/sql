package org.opensearch.graph.graphql;


import graphql.language.*;
import graphql.scalars.ExtendedScalars;
import graphql.schema.*;
import graphql.schema.idl.*;
import javaslang.Tuple2;
import org.opensearch.graph.ontology.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graphql.Scalars.*;
import static graphql.schema.GraphQLSchema.newSchema;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static org.opensearch.graph.graphql.GraphQLToOntologyTransformer.WhereSupportGraphQL.*;

public class GraphQLToOntologyTransformer implements OntologyTransformerIfc<String, Ontology>, GraphQLSchemaUtils {
    //where input object
    public static final String QUERY = "Query";


    //graph QL reader and schema parts
    private GraphQLSchema graphQLSchema;
    private SchemaParser schemaParser = new SchemaParser();
    private TypeDefinitionRegistry typeRegistry = new TypeDefinitionRegistry();
    private SchemaGenerator schemaGenerator = new SchemaGenerator();

    private Set<String> languageTypes = new HashSet<>();
    private Set<String> objectTypes = new HashSet<>();
    private Set<Property> properties = new HashSet<>();

    public GraphQLToOntologyTransformer() {
        languageTypes.addAll(Arrays.asList(QUERY, WHERE_OPERATOR, WHERE_CLAUSE, CONSTRAINT));
    }


    /**
     * get the graph QL schema
     *
     * @return
     */
    @Override
    public GraphQLSchema getGraphQLSchema() {
        return graphQLSchema;
    }

    public TypeDefinitionRegistry getTypeRegistry() {
        return typeRegistry;
    }


    /**
     * API that will transform a GraphQL schema into opengraph ontology schema
     *
     * @param source
     * @return
     */
    public Ontology transform(String ontologyName, String source) throws RuntimeException {
        try {
            Ontology ontology = transform(new FileInputStream(source));
            ontology.setOnt(ontologyName);
            return ontology;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * API that will translate a opengraph ontology schema to GraphQL schema
     */
    public String translate(Ontology source) {
        //Todo
        throw new RuntimeException("Not Implemented");
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
                        .type(type(accessor.property$(p).getType(), accessor))
                        .definition(new FieldDefinition(p, new TypeName(accessor.property$(p).getType())))
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

    /**
     * API that will transform a GraphQL schema into opengraph ontology schema
     *
     * @param streams
     * @return
     */
    public Ontology transform(InputStream... streams) {
        if (graphQLSchema == null) {
            // each registry is merged into the main registry
            Arrays.asList(streams).forEach(s -> typeRegistry.merge(schemaParser.parse(new InputStreamReader(s))));
            //create schema
            RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring()
                    .scalar(ExtendedScalars.GraphQLLong)
                    .scalar(ExtendedScalars.Json)
                    .scalar(ExtendedScalars.Object)
                    .scalar(ExtendedScalars.Url)
                    .scalar(ExtendedScalars.DateTime)
                    .scalar(ExtendedScalars.Time);
            graphQLSchema = schemaGenerator.makeExecutableSchema(
                    SchemaGenerator.Options.defaultOptions(),
                    typeRegistry,
                    builder.build());
        }
        //create a curated list of names for typed schema elements
        return transform(graphQLSchema);

    }

    /**
     * @param graphQLSchema
     * @return
     */
    public Ontology transform(GraphQLSchema graphQLSchema) {
        validateLanguageType(graphQLSchema);
        populateObjectTypes(graphQLSchema);

        //transform
        Ontology.OntologyBuilder builder = Ontology.OntologyBuilder.anOntology();
        interfaces(graphQLSchema, builder);
        entities(graphQLSchema, builder);
        relations(graphQLSchema, builder);
        properties(graphQLSchema, builder);
        enums(graphQLSchema, builder);

        return builder.build();
    }

    private void validateLanguageType(GraphQLSchema graphQLSchema) {
        List<GraphQLNamedType> types = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> languageTypes.contains(p.getName()))
                .collect(Collectors.toList());

        if (types.size() != languageTypes.size())
            throw new IllegalArgumentException("GraphQL schema doesnt include Query/Where types");
    }

    private void populateObjectTypes(GraphQLSchema graphQLSchema) {
        objectTypes.addAll(Stream.concat(graphQLSchema.getAllTypesAsList().stream()
                                .filter(p -> GraphQLInterfaceType.class.isAssignableFrom(p.getClass()))
                                .map(GraphQLNamedSchemaElement::getName),
                        graphQLSchema.getAllTypesAsList().stream()
                                .filter(p -> GraphQLObjectType.class.isAssignableFrom(p.getClass()))
                                .map(GraphQLNamedSchemaElement::getName)
                )
                .filter(p -> !p.startsWith("__"))
                .filter(p -> !languageTypes.contains(p))
                .collect(Collectors.toList()));
    }

    private List<Property> populateProperties(List<GraphQLFieldDefinition> fieldDefinitions) {
        Set<Property> collect = fieldDefinitions.stream()
                .filter(p -> Type.class.isAssignableFrom(p.getDefinition().getType().getClass()))
                .map(p -> createProperty(p.getDefinition().getType(), p.getName()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

        //add properties to context properties set
        properties.addAll(collect);
        return new ArrayList<>(collect);
    }


    /**
     * filter entity type according to predicate
     *
     * @param type
     * @param predicate
     * @return
     */
    private Optional<Tuple2<String, TypeName>> filter(Type type, String field, Predicate<TypeName> predicate) {
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
     * populate property type according to entities
     *
     * @param type
     * @param fieldName
     * @return
     */
    private Optional<Property> createProperty(Type type, String fieldName) {
        //scalar type property
        if ((type instanceof TypeName) &&
                (!objectTypes.contains(((TypeName) type).getName()))) {
            return Optional.of(new Property(fieldName, fieldName, ((TypeName) type).getName()));
        }

        //list type
        if (type instanceof ListType) {
            return createProperty(((ListType) type).getType(), fieldName);
        }
        //non null type - may contain all sub-types (wrapper)
        if (type instanceof NonNullType) {
            Type rawType = ((NonNullType) type).getType();

            //validate only scalars are registered as properties
            if ((rawType instanceof TypeName) &&
                    (!objectTypes.contains(((TypeName) rawType).getName()))) {
                return Property.MandatoryProperty.of(Optional.of(new Property(fieldName, fieldName, ((TypeName) rawType).getName())));
            }

            if (rawType instanceof ListType) {
                return Property.MandatoryProperty.of(createProperty(((ListType) rawType).getType(), fieldName));
            }
        }

        return Optional.empty();
    }

    /**
     * generate interface entity types
     *
     * @param graphQLSchema
     * @param context
     * @return
     */
    private Ontology.OntologyBuilder interfaces(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder context) {
        List<EntityType> collect = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> GraphQLInterfaceType.class.isAssignableFrom(p.getClass()))
                .map(ifc -> createEntity(ifc.getName(), ((GraphQLInterfaceType) ifc).getFieldDefinitions()))
                .collect(Collectors.toList());
        return context.addEntityTypes(collect);
    }

    /**
     * generate concrete entity types
     *
     * @param graphQLSchema
     * @param context
     * @return
     */
    private Ontology.OntologyBuilder entities(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder context) {
        List<EntityType> collect = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> GraphQLObjectType.class.isAssignableFrom(p.getClass()))
                .filter(p -> !languageTypes.contains(p.getName()))
                .filter(p -> !p.getName().startsWith("__"))
                .map(ifc -> createEntity(ifc.getName(), ((GraphQLObjectType) ifc).getFieldDefinitions()))
                .collect(Collectors.toList());
        return context.addEntityTypes(collect);
    }

    /**
     * generate entity (interface) type
     *
     * @return
     */
    private EntityType createEntity(String name, List<GraphQLFieldDefinition> fields) {
        List<Property> properties = populateProperties(fields);

        EntityType.Builder builder = EntityType.Builder.get();
        builder.withName(name).withEType(name);
        builder.withProperties(properties.stream().map(Property::getName).collect(Collectors.toList()));
        builder.withMandatory(properties.stream().filter(p -> p instanceof Property.MandatoryProperty).map(Property::getName).collect(Collectors.toList()));

        return builder.build();
    }

    private Ontology.OntologyBuilder relations(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder context) {
        Map<String, List<RelationshipType>> collect = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> GraphQLObjectType.class.isAssignableFrom(p.getClass()))
                .filter(p -> !languageTypes.contains(p.getName()))
                .filter(p -> !p.getName().startsWith("__"))
                .map(ifc -> createRelation(ifc.getName(), ((GraphQLObjectType) ifc).getFieldDefinitions()))
                .flatMap(p -> p.stream())
                .collect(Collectors.groupingBy(RelationshipType::getrType));

        //merge e-pairs
        collect.forEach((key, value) -> {
            List<EPair> pairs = value.stream()
                    .flatMap(ep -> ep.getePairs().stream())
                    .collect(Collectors.toList());
            //replace multi relationships with one containing all epairs
            context.addRelationshipType(value.get(0).withEPairs(pairs.toArray(new EPair[0])));
        });
        return context;
    }

    /**
     * @param name
     * @param fieldDefinitions
     * @return
     */
    private List<RelationshipType> createRelation(String name, List<GraphQLFieldDefinition> fieldDefinitions) {
        Set<Tuple2<String, TypeName>> typeNames = fieldDefinitions.stream()
                .filter(p -> Type.class.isAssignableFrom(p.getDefinition().getType().getClass()))
                .map(p -> filter(p.getDefinition().getType(), p.getName(), type -> objectTypes.contains(type.getName())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
        //relationships for each entity
        List<RelationshipType> collect = typeNames.stream()
                .map(type -> RelationshipType.Builder.get()
                        //nested objects are directional by nature (nesting dictates the direction)
                        .withDirectional(true)
                        .withName(type._1())
                        .withRType(type._1())
                        .withEPairs(Collections.singletonList(new EPair(name, type._2().getName())))
                        .build())
                .collect(Collectors.toList());

        return collect;
    }

    private Ontology.OntologyBuilder properties(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder context) {
        context.withProperties(new HashSet<>(properties));
        return context;
    }

    /**
     * @param graphQLSchema
     * @param context
     * @return
     */
    private Ontology.OntologyBuilder enums(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder context) {
        List<EnumeratedType> collect = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> GraphQLEnumType.class.isAssignableFrom(p.getClass()))
                .filter(p -> !languageTypes.contains(p.getName()))
                .filter(p -> !p.getName().startsWith("__"))
                .map(ifc -> createEnum((GraphQLEnumType) ifc))
                .collect(Collectors.toList());

        context.withEnumeratedTypes(collect);
        return context;
    }

    private EnumeratedType createEnum(GraphQLEnumType ifc) {
        AtomicInteger counter = new AtomicInteger(0);
        EnumeratedType.EnumeratedTypeBuilder builder = EnumeratedType.EnumeratedTypeBuilder.anEnumeratedType();
        builder.withEType(ifc.getName());
        builder.withValues(ifc.getValues().stream()
                .map(v -> new org.opensearch.graph.ontology.Value(counter.getAndIncrement(), v.getName()))
                .collect(Collectors.toList()));
        return builder.build();
    }

    static class WhereSupportGraphQL {
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

package org.opensearch.graph.graphql;


import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.scalars.ExtendedScalars;
import graphql.schema.*;
import graphql.schema.idl.*;
import javaslang.Tuple2;
import org.opensearch.graph.ontology.*;
import org.opensearch.graph.ontology.PrimitiveType.Types;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graphql.Scalars.GraphQLString;
import static org.opensearch.graph.graphql.GraphQLSchemaUtils.filter;

public class GraphQLToOntologyTransformer implements OntologyTransformerIfc<String, Ontology>, GraphQLSchemaUtils {


    //graph QL reader and schema parts
    private GraphQLSchema graphQLSchema;
    private SchemaParser schemaParser = new SchemaParser();
    private TypeDefinitionRegistry typeRegistry = new TypeDefinitionRegistry();
    private SchemaGenerator schemaGenerator = new SchemaGenerator();

    private Set<String> languageTypes = new HashSet<>();
    private Set<String> objectTypes = new HashSet<>();
    private Set<Property> properties = new HashSet<>();

    public GraphQLToOntologyTransformer() {
        languageTypes.addAll(Arrays.asList(QUERY));
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
                    .wiringFactory(new EchoingWiringFactory())
                    .scalar(ExtendedScalars.newAliasedScalar("IP")
                            .aliasedScalar(GraphQLString)
                            .build())
                    .scalar(ExtendedScalars.newAliasedScalar("GeoPoint")
                            .aliasedScalar(GraphQLString)
                            .build())
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
        primitives(graphQLSchema, builder);
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
     * generate schema primitives
     *
     * @param graphQLSchema
     * @param builder
     */
    private void primitives(GraphQLSchema graphQLSchema, Ontology.OntologyBuilder builder) {
        Set<PrimitiveType> types = graphQLSchema.getAllTypesAsList().stream()
                .filter(p -> GraphQLScalarType.class.isAssignableFrom(p.getClass()))
                .filter(p -> !Types.contains(p.getName().toUpperCase()))
                .map(p -> createPrimitive((GraphQLScalarType) p))
                .collect(Collectors.toSet());
        builder.withPrimitives(types);
    }

    private PrimitiveType createPrimitive(GraphQLScalarType scalar) {
        return new PrimitiveType(scalar.getName().toLowerCase(),scalar.getDefinition().getClass());
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
                .map(ifc -> createInterface(((GraphQLInterfaceType) ifc)))
                .collect(Collectors.toList());
        return context.addEntityTypes(collect);
    }

    private EntityType createInterface(GraphQLInterfaceType ifc) {
        List<Property> properties = populateProperties(ifc.getFieldDefinitions());
        EntityType.Builder builder = EntityType.Builder.get();
        builder.withName(ifc.getName()).withEType(ifc.getName());
        builder.isAbstract(true);
        builder.withProperties(properties.stream().map(Property::getName).collect(Collectors.toList()));
        builder.withMandatory(properties.stream()
                .filter(p -> p instanceof Property.MandatoryProperty).map(Property::getName).collect(Collectors.toList()));

        return builder.build();

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
                .map(ifc -> createEntity((GraphQLObjectType) ifc, context))
                .collect(Collectors.toList());
        return context.addEntityTypes(collect);
    }

    /**
     * generate entity (interface) type
     *
     * @return
     */
    private EntityType createEntity(GraphQLObjectType object, Ontology.OntologyBuilder context) {
        List<Property> properties = populateProperties(object.getFieldDefinitions());
        EntityType.Builder builder = EntityType.Builder.get();
        builder.withName(object.getName()).withEType(object.getName());
        builder.withParentTypes(object.getInterfaces().stream()
                .filter(p -> context.getEntityType(p.getName()).isPresent())
                .map(p -> context.getEntityType(p.getName()).get().geteType()).collect(Collectors.toList()));
        builder.withProperties(properties.stream().map(Property::getName).collect(Collectors.toList()));
        builder.withMandatory(properties.stream()
                .filter(p -> p instanceof Property.MandatoryProperty).map(Property::getName).collect(Collectors.toList()));

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

}

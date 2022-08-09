package org.opensearch.graph.graphql;

import graphql.schema.GraphQLSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.graph.ontology.EnumeratedType;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.Property;
import org.opensearch.graph.ontology.Value;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import static org.opensearch.graph.ontology.Property.equal;


public class GraphQLOntologyTranslatorTest {
    public static Ontology ontology;
    public static GraphQLSchema graphQLSchema;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream baseSchemaInput = new FileInputStream( "schema/logs/agent.graphql");
        InputStream agentSchemaInput = new FileInputStream( "schema/logs/agent.graphql");
        GraphQLToOntologyTransformer transformer = new GraphQLToOntologyTransformer();

        ontology = transformer.transform(baseSchemaInput, agentSchemaInput);
        graphQLSchema = transformer.getGraphQLSchema();

        Assertions.assertNotNull(ontology);
    }

    @Test
    public void testEnumTranslation() {
        Assertions.assertEquals(ontology.getEnumeratedTypes().size(), 1);
        Ontology.Accessor accessor = new Ontology.Accessor(ontology);

        Assertions.assertEquals(accessor.enumeratedType$("Episode"),
                new EnumeratedType("Episode",
                        Arrays.asList(new Value(0, "NEWHOPE"),
                                new Value(1, "EMPIRE"),
                                new Value(2, "JEDI"))));


    }

    @Test
    public void testPropertiesTranslation() {
        Assertions.assertEquals(ontology.getProperties().size(), 6);
        Ontology.Accessor accessor = new Ontology.Accessor(ontology);

        Assertions.assertTrue(equal(accessor.property$("id"), new Property.MandatoryProperty(new Property("id", "id", "ID"))));
        Assertions.assertTrue(equal(accessor.property$("name"), new Property.MandatoryProperty(new Property("name", "name", "String"))));
        Assertions.assertTrue(equal(accessor.property$("appearsIn"), new Property.MandatoryProperty(new Property("appearsIn", "appearsIn", "Episode"))));
        Assertions.assertTrue(equal(accessor.property$("description"), new Property("description", "description", "String")));
        Assertions.assertTrue(equal(accessor.property$("primaryFunction"), new Property("primaryFunction", "primaryFunction", "String")));
        Assertions.assertTrue(equal(accessor.property$("homePlanet"), new Property("homePlanet", "homePlanet", "String")));
    }

    @Test
    public void testEntitiesTranslation() {
        Assertions.assertEquals(ontology.getEntityTypes().size(), 3);
        Ontology.Accessor accessor = new Ontology.Accessor(ontology);

        Assertions.assertEquals(accessor.entity$("Droid").geteType(), "Droid");
        Assertions.assertEquals(accessor.entity$("Droid").getProperties().size(), 5);
        Assertions.assertEquals(accessor.entity$("Droid").getMandatory().size(), 3);

        Assertions.assertEquals(accessor.entity$("Human").geteType(), "Human");
        Assertions.assertEquals(accessor.entity$("Human").getProperties().size(), 5);
        Assertions.assertEquals(accessor.entity$("Human").getMandatory().size(), 3);

        Assertions.assertEquals(accessor.entity$("Character").geteType(), "Character");
        Assertions.assertEquals(accessor.entity$("Character").getProperties().size(), 4);
        Assertions.assertEquals(accessor.entity$("Character").getMandatory().size(), 3);

    }

    @Test
    public void testRelationsTranslation() {
        Assertions.assertEquals(ontology.getRelationshipTypes().size(), 2);
        Ontology.Accessor accessor = new Ontology.Accessor(ontology);

        Assertions.assertEquals(accessor.relation$("owns").getrType(), "owns");
        Assertions.assertEquals(accessor.relation$("owns").getePairs().size(), 1);

        Assertions.assertEquals(accessor.relation$("friends").getrType(), "friends");
        Assertions.assertEquals(accessor.relation$("friends").getePairs().size(), 2);

    }

    @Test
    public void testOntology2GraphQLTransformation() {
        GraphQLSchema targetSchema = new GraphQLToOntologyTransformer().transform(ontology);
        Ontology ontologyTarget = new GraphQLToOntologyTransformer().transform(targetSchema);

        Assertions.assertEquals(ontology.getEntityTypes(), ontologyTarget.getEntityTypes());
        Assertions.assertEquals(ontology.getRelationshipTypes(), ontologyTarget.getRelationshipTypes());
        Assertions.assertEquals(ontology.getProperties(), ontologyTarget.getProperties());
        Assertions.assertEquals(ontology.getEnumeratedTypes(), ontologyTarget.getEnumeratedTypes());

        Assertions.assertEquals(targetSchema.getQueryType().getFieldDefinitions().size()
                , graphQLSchema.getQueryType().getFieldDefinitions().size());
        Assertions.assertEquals(targetSchema.getAllTypesAsList().size()
                , graphQLSchema.getAllTypesAsList().size());

    }

}

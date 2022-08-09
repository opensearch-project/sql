package org.opensearch.graph.graphql;

import graphql.schema.GraphQLSchema;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.graph.ontology.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import static org.opensearch.graph.ontology.Property.equal;


public class GraphQLOntologyAgentTranslatorTest {
    public static Ontology ontology;
    public static Ontology.Accessor ontologyAccessor;
    public static GraphQLSchema graphQLSchema;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream baseSchemaInput = new FileInputStream("schema/logs/base.graphql");
        InputStream agentSchemaInput = new FileInputStream("schema/logs/agent.graphql");
        GraphQLToOntologyTransformer transformer = new GraphQLToOntologyTransformer();

        ontology = transformer.transform(baseSchemaInput, agentSchemaInput);
        ontologyAccessor = new Ontology.Accessor(ontology);
        graphQLSchema = transformer.getGraphQLSchema();
        Assertions.assertNotNull(ontology);
    }

    @Test
    public void testEnumTranslation() {
        Assertions.assertEquals(ontologyAccessor.enumeratedType$("StreamType"),
                new EnumeratedType("StreamType",
                        Arrays.asList(new Value(0, "logs"),
                                new Value(1, "metrics"),
                                new Value(2, "traces"),
                                new Value(3, "synthetics"))));
    }

    @Test
    @Ignore("todo - express graph ql scalar in java meaningfull manner")
    public void testSamplePrimitivesTranslation() {
        Assertions.assertEquals(ontologyAccessor.primitiveType$("url"),
                new PrimitiveType("url", String.class));
    }

    @Test
    public void testSamplePropertiesTranslation() {
        Assertions.assertTrue(equal(ontologyAccessor.property$("id"), new Property.MandatoryProperty(new Property("id", "id", "ID"))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("name"), new Property.MandatoryProperty(new Property("name", "name", "String"))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("timestamp"), new Property("timestamp", "timestamp", "Time")));
        Assertions.assertTrue(equal(ontologyAccessor.property$("labels"), new Property("labels", "labels", "JSON")));
        Assertions.assertTrue(equal(ontologyAccessor.property$("location"), new Property("location", "location", "GeoPoint")));
    }

    @Test
    @Ignore
    public void testAgentEntityTranslation() {

        Assertions.assertEquals(ontologyAccessor.entity$("Droid").geteType(), "Droid");
        Assertions.assertEquals(ontologyAccessor.entity$("Droid").getProperties().size(), 5);
        Assertions.assertEquals(ontologyAccessor.entity$("Droid").getMandatory().size(), 3);

        Assertions.assertEquals(ontologyAccessor.entity$("Human").geteType(), "Human");
        Assertions.assertEquals(ontologyAccessor.entity$("Human").getProperties().size(), 5);
        Assertions.assertEquals(ontologyAccessor.entity$("Human").getMandatory().size(), 3);

        Assertions.assertEquals(ontologyAccessor.entity$("Character").geteType(), "Character");
        Assertions.assertEquals(ontologyAccessor.entity$("Character").getProperties().size(), 4);
        Assertions.assertEquals(ontologyAccessor.entity$("Character").getMandatory().size(), 3);

    }

    @Test
    @Ignore
    public void testRelationsTranslation() {
        Assertions.assertEquals(ontologyAccessor.relation$("owns").getrType(), "owns");
        Assertions.assertEquals(ontologyAccessor.relation$("owns").getePairs().size(), 1);

        Assertions.assertEquals(ontologyAccessor.relation$("friends").getrType(), "friends");
        Assertions.assertEquals(ontologyAccessor.relation$("friends").getePairs().size(), 2);

    }
}

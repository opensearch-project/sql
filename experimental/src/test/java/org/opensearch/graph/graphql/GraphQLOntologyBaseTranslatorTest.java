package org.opensearch.graph.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.opensearch.graph.ontology.PrimitiveType.Types.*;
import static org.opensearch.graph.ontology.Property.equal;


public class GraphQLOntologyBaseTranslatorTest {
    public static Ontology ontology;
    public static Ontology.Accessor ontologyAccessor;
    public static GraphQLSchema graphQLSchema;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream baseSchemaInput = new FileInputStream("schema/logs/base.graphql");
        GraphQLToOntologyTransformer transformer = new GraphQLToOntologyTransformer();

        ontology = transformer.transform(baseSchemaInput);
        ontologyAccessor = new Ontology.Accessor(ontology);
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
    public void testSamplePropertiesTranslation() {
        Assertions.assertTrue(equal(ontologyAccessor.property$("id"), new Property.MandatoryProperty(new Property("id", "id", ID.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("name"), new Property.MandatoryProperty(new Property("name", "name", STRING.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("timestamp"), new Property("timestamp", "timestamp", TIME.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("labels"), new Property("labels", "labels", JSON.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("location"), new Property("location", "location", GEOPOINT.tlc())));
    }

    @Test
    public void testEntityTranslation() {
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").isAbstract(), true);
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").geteType(), "BaseRecord");
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").getProperties().size(), 5);
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").getMandatory().size(), 1);
    }
}

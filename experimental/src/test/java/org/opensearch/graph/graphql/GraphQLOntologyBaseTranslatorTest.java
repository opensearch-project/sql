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

        ontology = transformer.transform("base",baseSchemaInput);
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
        Assertions.assertTrue(equal(ontologyAccessor.property$("id"), new Property.MandatoryProperty(new Property("id", "id", ID.asType()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("name"), new Property.MandatoryProperty(new Property("name", "name", STRING.asType()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("timestamp"), new Property("timestamp", "timestamp", TIME.asType())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("labels"), new Property("labels", "labels", JSON.asType())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("location"), new Property("location", "location", GEOPOINT.asType())));
    }

    @Test
    public void testEntityTranslation() {
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").isAbstract(), true);
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").geteType(), "BaseRecord");
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").getProperties().size(), 5);
        Assertions.assertEquals(ontologyAccessor.entity$("BaseRecord").getMandatory().size(), 1);

        Assertions.assertEquals(ontologyAccessor.entity$("AutonomousSystem").isAbstract(), false);
        Assertions.assertEquals(ontologyAccessor.entity$("AutonomousSystem").geteType(), "AutonomousSystem");
        Assertions.assertEquals(ontologyAccessor.entity$("AutonomousSystem").getIdField().size(), 0);//todo - fix according to the @Key directive
        Assertions.assertEquals(ontologyAccessor.entity$("AutonomousSystem").getProperties().size(), 2);
        Assertions.assertEquals(ontologyAccessor.entity$("AutonomousSystem").getMandatory().size(), 1);


    }
}

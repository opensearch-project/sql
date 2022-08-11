package org.opensearch.graph.graphql;

import graphql.schema.GraphQLSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.ontology.EnumeratedType;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.Property;
import org.opensearch.graph.ontology.Value;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import static org.opensearch.graph.ontology.PrimitiveType.Types.*;
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
        Assertions.assertNotNull(ontology);
        ontologyAccessor = new Ontology.Accessor(ontology);
    }

    /**
     * test creation of an index provider using the predicate conditions for top level entity will be created an index
     */
    @Test
    public void testIndexProviderBuilder() {
        IndexProvider provider = IndexProvider.Builder.generate(ontology
                , e -> e.getDirectives().stream().anyMatch(d -> d.getName().equals("model"))
                , r -> r.getDirectives().stream()
                        .anyMatch(d -> d.getName().equals("relation") && d.containsArgVal("foreign")));

        Assertions.assertEquals(provider.getEntities().size(),1);
        Assertions.assertEquals(provider.getRelations().size(),0);
    }

    @Test
    public void testEnumTranslation() {
        Assertions.assertEquals(ontologyAccessor.enumeratedType$("AgentIdStatus"),
                new EnumeratedType("AgentIdStatus",
                        Arrays.asList(new Value(0, "verified"),
                                new Value(1, "mismatch"),
                                new Value(2, "missing"),
                                new Value(3, "auth_metadata_missing"))));
    }

    @Test
    public void testSamplePropertiesTranslation() {
        Assertions.assertTrue(equal(ontologyAccessor.property$("id"),
                new Property.MandatoryProperty(new Property("id", "id", ID.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("name"),
                new Property.MandatoryProperty(new Property("name", "name", STRING.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("labels"),
                new Property.MandatoryProperty(new Property("labels", "labels", JSON.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("tags"),
                new Property.MandatoryProperty(new Property("tags", "tags", LIST_OF_STRING.tlc()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("aType"),
                new Property("aType", "aType", STRING.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("version"),
                new Property("version", "version", STRING.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("number"),
                new Property("number", "number", LONG.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("timestamp"),
                new Property("timestamp", "timestamp", TIME.tlc())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("location"),
                new Property("location", "location", GEOPOINT.tlc())));
    }

    @Test
    public void testAgentEntityTranslation() {
        Assertions.assertEquals(ontologyAccessor.entity$("Agent").geteType(), "Agent");
        Assertions.assertEquals(ontologyAccessor.entity$("Agent").getProperties().size(), 11);
        Assertions.assertEquals(ontologyAccessor.entity$("Agent").getMandatory().size(), 2);
    }
}

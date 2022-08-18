package org.opensearch.graph.graphql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.Assert;
import graphql.schema.GraphQLSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.ontology.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

import static org.opensearch.graph.ontology.PrimitiveType.Types.*;
import static org.opensearch.graph.ontology.Property.equal;


public class GraphQLOntologyUserTranslatorTest {
    public static Ontology ontology;
    public static Ontology.Accessor ontologyAccessor;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream baseSchemaInput = new FileInputStream("schema/logs/base.graphql");
        InputStream userSchemaInput = new FileInputStream("schema/logs/user.graphql");
        GraphQLToOntologyTransformer transformer = new GraphQLToOntologyTransformer();

        ontology = transformer.transform("user",baseSchemaInput,userSchemaInput);
        ontologyAccessor = new Ontology.Accessor(ontology);
        Assertions.assertNotNull(ontology);
        String valueAsString = new ObjectMapper().writeValueAsString(ontology);
        Assertions.assertNotNull(valueAsString);
    }

    @Test
    public void testSamplePropertiesTranslation() {
        Assertions.assertTrue(equal(ontologyAccessor.property$("id"), new Property.MandatoryProperty(new Property("id", "id", ID.asType()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("name"), new Property.MandatoryProperty(new Property("name", "name", STRING.asType()))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("group"), new Property.MandatoryProperty(new Property("group", "Group", ObjectType.of("Group")))));
        Assertions.assertTrue(equal(ontologyAccessor.property$("email"), new Property("email", "email", STRING.asType())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("fullName"), new Property("fullName", "fullName", STRING.asType())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("roles"), new Property("roles", "roles", STRING.asListType())));
    }

    @Test
    public void testEntityTranslation() {
        Assertions.assertEquals(ontologyAccessor.entity$("User").isAbstract(), false);
        Assertions.assertEquals(ontologyAccessor.entity$("User").getIdField().size(), 1);
        Assertions.assertEquals(ontologyAccessor.entity$("User").getIdField().get(0), "id");
        Assertions.assertEquals(ontologyAccessor.entity$("User").geteType(), "User");
        Assertions.assertEquals(ontologyAccessor.entity$("User").getProperties().size(), 8);
        Assertions.assertEquals(ontologyAccessor.entity$("User").getMandatory().size(), 1);
        //implemented as nested object
        Assertions.assertTrue(equal(ontologyAccessor.property$("group"), new Property("group", "Group", ObjectType.of("Group"))));
    }

    @Test
    public void testRelationTranslation() {
        Assertions.assertEquals(ontologyAccessor.relations().size(), 1);
        Assertions.assertEquals(ontologyAccessor.entity$("Group").geteType(), "Group");
        Assertions.assertEquals(ontologyAccessor.entity$("Group").getIdField().size(), 1);
        Assertions.assertEquals(ontologyAccessor.entity$("Group").getIdField().get(0), "id");
        Assertions.assertEquals(ontologyAccessor.entity$("Group").getProperties().size(), 3);
        Assertions.assertEquals(ontologyAccessor.entity$("Group").getMandatory().size(), 1);
    }

     /**
     * test creation of an index provider using the predicate conditions for top level entity will be created an index
     */
    @Test
    public void testIndexProviderBuilder() throws Exception {
        IndexProvider provider = IndexProvider.Builder.generate(ontology
                , e -> e.getDirectives().stream().anyMatch(d -> d.getName().equals("model"))
                , r -> r.getDirectives().stream()
                        .anyMatch(d -> d.getName().equals("relation") && d.containsArgVal("foreign")));

        String valueAsString = new ObjectMapper().writeValueAsString(provider);
        Assert.assertNotNull(valueAsString);
        Assertions.assertEquals(provider.getRootEntities().size(),1);
        Assertions.assertEquals(provider.getRootEntities().get(0).getType(),"User");
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().size(),1);
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().get(0).getType(),"Group");
        Assertions.assertEquals(provider.getRelations().size(),0);
    }

}

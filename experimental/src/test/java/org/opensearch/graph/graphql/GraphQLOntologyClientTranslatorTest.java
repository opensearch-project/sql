package org.opensearch.graph.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.ontology.ObjectType;
import org.opensearch.graph.ontology.Ontology;
import org.opensearch.graph.ontology.Property;

import java.io.FileInputStream;
import java.io.InputStream;

import static org.opensearch.graph.ontology.PrimitiveType.Types.*;
import static org.opensearch.graph.ontology.Property.equal;


public class GraphQLOntologyClientTranslatorTest {
    public static Ontology ontology;
    public static Ontology.Accessor ontologyAccessor;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream baseSchemaInput = new FileInputStream("schema/logs/base.graphql");
        InputStream userSchemaInput = new FileInputStream("schema/logs/user.graphql");
        InputStream clientSchemaInput = new FileInputStream("schema/logs/client.graphql");
        GraphQLToOntologyTransformer transformer = new GraphQLToOntologyTransformer();

        ontology = transformer.transform("user",baseSchemaInput,userSchemaInput,clientSchemaInput);
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
        Assertions.assertTrue(equal(ontologyAccessor.property$("location"), new Property("location", "location", GEOPOINT.asType())));
        Assertions.assertTrue(equal(ontologyAccessor.property$("geo"), new Property("geo", "Geo", ObjectType.of("Geo"))));
        //implemented as nested object
        Assertions.assertTrue(equal(ontologyAccessor.property$("user"), new Property("user", "User", ObjectType.of("User"))));
    }

    @Test
    public void testEntityTranslation() {
        Assertions.assertEquals(ontologyAccessor.entity$("Client").isAbstract(), false);
        Assertions.assertEquals(ontologyAccessor.entity$("Client").geteType(), "Client");
        Assertions.assertEquals(ontologyAccessor.entity$("Client").getIdField().size(), 0);//todo - fix according to @Key directive
        Assertions.assertEquals(ontologyAccessor.entity$("Client").getProperties().size(), 21);
        Assertions.assertEquals(ontologyAccessor.entity$("Client").getMandatory().size(), 1);
    }

    @Test
    public void testRelationTranslation() {
        Assertions.assertEquals(ontologyAccessor.relation$("has_User").getrType(), "has_User");
        Assertions.assertEquals(ontologyAccessor.relation$("has_User").getePairs().get(0).getSideAFieldName(), "user");

        Assertions.assertEquals(ontologyAccessor.entity$("User").geteType(), "User");
        Assertions.assertEquals(ontologyAccessor.entity$("User").getIdField().size(), 1);
        Assertions.assertEquals(ontologyAccessor.entity$("User").getIdField().get(0), "id");
        Assertions.assertEquals(ontologyAccessor.entity$("User").getProperties().size(), 8);
        Assertions.assertEquals(ontologyAccessor.entity$("User").getMandatory().size(), 1);


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
        Assertions.assertEquals(provider.getRootEntities().size(),2);
        Assertions.assertEquals(provider.getRootEntities().get(0).getType(),"Client");
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().size(),3);
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().get(0).getType(),"User");
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().get(1).getType(),"AutonomousSystem");
        Assertions.assertEquals(provider.getRootEntities().get(0).getNested().get(2).getType(),"Geo");
        Assertions.assertEquals(provider.getRelations().size(),0);
    }

}

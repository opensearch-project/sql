package org.opensearch.graph.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.NoOpClient;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.index.template.PutIndexTemplateRequestBuilder;
import org.opensearch.graph.index.transform.IndexEntitiesMappingBuilder;
import org.opensearch.graph.ontology.Ontology;

import java.io.InputStream;
import java.util.*;

public class MappingSimpleEntityTemplateGeneratorTest {
    static Ontology ontology;
    static IndexProvider indexProvider;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ontology/user.json");
        ontology = new ObjectMapper().readValue(stream, Ontology.class);
        indexProvider = IndexProvider.Builder.generate(ontology
                , e -> e.getDirectives().stream()
                        .anyMatch(d -> d.getName().equals("model"))
                , r -> r.getDirectives().stream()
                        .anyMatch(d -> d.getName().equals("relation") && d.containsArgVal("foreign")));
    }

    @Test
    @Ignore
    public void GenerateAgentEntityMappingTest() {
        IndexEntitiesMappingBuilder builder = new IndexEntitiesMappingBuilder(indexProvider);
        HashMap<String, PutIndexTemplateRequestBuilder> requests = new HashMap<>();
        Collection<PutIndexTemplateRequestBuilder> results = builder.map(new Ontology.Accessor(ontology), new NoOpClient("test"), requests);
        Assert.assertNotNull(requests.get("user"));
        Assert.assertEquals(1, requests.get("user").getMappings().size());
        Assert.assertNotNull(requests.get("user").getMappings().get("User"));
        Assert.assertNotNull(((Map) requests.get("user").getMappings().get("User")).get("properties"));
        Assert.assertEquals(7, ((Map) ((Map) requests.get("user").getMappings().get("User")).get("properties")).size());
    }
}

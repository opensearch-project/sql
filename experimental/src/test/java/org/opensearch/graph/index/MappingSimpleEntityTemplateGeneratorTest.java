package org.opensearch.graph.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.NoOpClient;
import org.opensearch.graph.index.schema.IndexProvider;
import org.opensearch.graph.index.template.PutIndexTemplateRequestBuilder;
import org.opensearch.graph.index.transform.IndexEntitiesMappingBuilder;
import org.opensearch.graph.ontology.Ontology;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;

public class MappingSimpleEntityTemplateGeneratorTest {
    static Ontology ontology;
    static IndexProvider indexProvider;

    @BeforeAll
    public static void setUp() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ontology/agent.json");
        ontology = new ObjectMapper().readValue(stream, Ontology.class);
        indexProvider = IndexProvider.Builder.generate(ontology);
    }

    @Test
    public void GenerateAgentEntityMappingTest() {
        IndexEntitiesMappingBuilder builder = new IndexEntitiesMappingBuilder(indexProvider);
        HashMap<String, PutIndexTemplateRequestBuilder> requests = new HashMap<>();
        Collection<PutIndexTemplateRequestBuilder> results  = builder.map(new Ontology.Accessor(ontology), new NoOpClient("test"), requests);
        Assert.assertEquals(3,results.size());
    }
}

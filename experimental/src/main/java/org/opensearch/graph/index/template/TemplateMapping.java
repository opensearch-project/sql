package org.opensearch.graph.index.template;

import org.opensearch.client.Client;
import org.opensearch.graph.index.schema.BaseTypeElement;
import org.opensearch.graph.ontology.BaseElement;
import org.opensearch.graph.ontology.Ontology;

import java.util.Collection;
import java.util.Map;

public interface TemplateMapping<E extends BaseElement, T extends BaseTypeElement> {
    Collection<PutIndexTemplateRequestBuilder> map(Ontology.Accessor ontology, Client client, Map<String, PutIndexTemplateRequestBuilder> requests);
    Map<String, Object> generateElementMapping(Ontology.Accessor ontology, E element, T elementType, String label);
}

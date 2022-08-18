package org.opensearch.graph.index.template;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.graph.index.schema.BaseTypeElement;
import org.opensearch.graph.ontology.BaseElement;
import org.opensearch.graph.ontology.Ontology;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public interface TemplateMapping<E extends BaseElement, T extends BaseTypeElement> {
    Collection<PutIndexTemplateRequestBuilder> map(Ontology.Accessor ontology, Client client, Map<String, PutIndexTemplateRequestBuilder> requests);

    Map<String, Object> generateElementMapping(Ontology.Accessor ontology, E element, T elementType, String label);

    /**
     * create component template
     *
     * @param request
     * @return
     */
    static ComponentTemplate createComponentTemplate(PutIndexTemplateRequestBuilder request) {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.value(request.request().mappings());
            CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(builder), XContentType.JSON, ToXContent.EMPTY_PARAMS);
            new Template(request.request().settings(), mapping, Collections.EMPTY_MAP);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return null;
    }


}

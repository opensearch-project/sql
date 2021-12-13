package org.opensearch.sql.plugin.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class PutEventTransportAction extends HandledTransportAction<PutEventRequest, PutEventResponse> {

    private static final Logger log = LogManager.getLogger(PutEventTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public PutEventTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            Client client,
            ClusterService clusterService,
            Settings settings,
            NamedXContentRegistry xContentRegistry
    ) {
        super(PutEventAction.NAME, transportService, actionFilters, PutEventRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, PutEventRequest request,
                             ActionListener<PutEventResponse> listener) {
        client.index(indexRequest(request), new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(new PutEventResponse(RestStatus.OK));
            }

            @Override
            public void onFailure(Exception e) {
                log.error("index event {} failed", request, e);
                listener.onResponse(new PutEventResponse(RestStatus.BAD_REQUEST));
            }
        });
    }

    private IndexRequest indexRequest(PutEventRequest request) {
        Map<String, Object> metaInfo = new HashMap<>();
        Map<String, Object> content = new HashMap<>();

        metaInfo.put("type", "s3");
        metaInfo.put("bucket", request.getBucket());
        metaInfo.put("object", request.getObject());
        content.putAll(request.getTags());
        content.put("meta", metaInfo);
        final IndexRequest indexRequest = new IndexRequest(request.getIndex())
                .source(content);

        log.info("indexing {}", content);
        return indexRequest;
    }
}

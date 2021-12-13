package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class PutEventResponse extends ActionResponse implements ToXContentObject {

    private final RestStatus restStatus;

    public PutEventResponse(RestStatus restStatus) {
        this.restStatus = restStatus;
    }

    public PutEventResponse(StreamInput in) throws IOException {
        super(in);
        restStatus = in.readEnum(RestStatus.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
                .startObject()
                .field("status", restStatus)
                .endObject();
    }
}


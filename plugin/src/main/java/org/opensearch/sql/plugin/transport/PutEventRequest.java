package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Map;

public class PutEventRequest extends ActionRequest {

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getBucket() {
        return bucket;
    }

    public String getObject() {
        return object;
    }

    public Map<String, Object> getTags() {
        return tags;
    }

    private String index;
    private String type;
    private String bucket;
    private String object;
    private Map<String, Object> tags;

    public PutEventRequest(String index, String type, String bucket, String object,
                           Map<String, Object> tags) {
        this.index = index;
        this.type = type;
        this.bucket = bucket;
        this.object = object;
        this.tags = tags;
    }

    public PutEventRequest(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        type = in.readString();
        bucket = in.readString();
        object = in.readString();
        tags = in.readMap();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String toString() {
        return "PutEventRequest{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", bucket='" + bucket + '\'' +
                ", object='" + object + '\'' +
                ", tags=" + tags +
                '}';
    }
}

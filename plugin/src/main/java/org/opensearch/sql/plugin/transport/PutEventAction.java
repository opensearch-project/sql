package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

public class PutEventAction extends ActionType<PutEventResponse> {
    // External Action which used for public facing RestAPIs.
    public static final String NAME = "logstream/write";
    public static final PutEventAction INSTANCE = new PutEventAction();

    private PutEventAction() {
        super(NAME, PutEventResponse::new);
    }
}

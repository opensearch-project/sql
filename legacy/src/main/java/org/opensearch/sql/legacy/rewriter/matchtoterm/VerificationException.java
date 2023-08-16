/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.rewriter.matchtoterm;

import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;

public class VerificationException extends OpenSearchException {

    public VerificationException(String message) {
        super(message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}

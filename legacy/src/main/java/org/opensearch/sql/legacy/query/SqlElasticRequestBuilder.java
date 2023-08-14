/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.query;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.core.action.ActionResponse;

/**
 * Created by Eliran on 19/8/2015.
 */
public interface SqlElasticRequestBuilder {
    ActionRequest request();

    String explain();

    ActionResponse get();

    ActionRequestBuilder getBuilder();
}

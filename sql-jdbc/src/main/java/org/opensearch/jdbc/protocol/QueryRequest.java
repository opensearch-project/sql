/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import java.util.List;

public interface QueryRequest {

    String getQuery();

    List<? extends Parameter> getParameters();

    public int getFetchSize();

}

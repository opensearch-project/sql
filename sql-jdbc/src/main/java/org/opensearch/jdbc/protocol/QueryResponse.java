/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import java.util.List;

public interface QueryResponse {

    List<? extends ColumnDescriptor> getColumnDescriptors();

    List<List<Object>> getDatarows();

    long getTotal();

    long getSize();

    int getStatus();

    String getCursor();

    RequestError getError();
}

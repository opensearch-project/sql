/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc;

import java.sql.SQLException;

public interface OpenSearchConnection extends java.sql.Connection {

    String getClusterName() throws SQLException;

    String getClusterUUID() throws SQLException;

}

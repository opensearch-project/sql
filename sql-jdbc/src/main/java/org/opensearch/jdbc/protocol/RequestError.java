/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

public interface RequestError {
    String getReason();

    String getDetails();

    String getType();
}

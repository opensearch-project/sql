/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.protocol.ProtocolFactory;
import org.opensearch.jdbc.transport.http.HttpTransport;

/**
 * Factory to create JsonCursorHttpProtocol objects
 *
 *  @author abbas hussain
 *  @since 07.05.20
 */
public class JsonCursorHttpProtocolFactory implements ProtocolFactory<JsonCursorHttpProtocol, HttpTransport> {

    public static JsonCursorHttpProtocolFactory INSTANCE = new JsonCursorHttpProtocolFactory();

    private JsonCursorHttpProtocolFactory() {

    }

    @Override
    public JsonCursorHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonCursorHttpProtocol(transport);
    }
}

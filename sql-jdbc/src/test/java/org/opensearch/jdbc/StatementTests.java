/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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

package org.opensearch.jdbc;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.logging.NoOpLogger;
import org.opensearch.jdbc.protocol.ConnectionResponse;
import org.opensearch.jdbc.protocol.JdbcQueryRequest;
import org.opensearch.jdbc.protocol.Protocol;
import org.opensearch.jdbc.protocol.ProtocolFactory;
import org.opensearch.jdbc.protocol.QueryRequest;
import org.opensearch.jdbc.protocol.QueryResponse;
import org.opensearch.jdbc.protocol.exceptions.InternalServerErrorException;
import org.opensearch.jdbc.protocol.exceptions.ResponseException;
import org.opensearch.jdbc.test.PerTestWireMockServerExtension;
import org.opensearch.jdbc.transport.Transport;
import org.opensearch.jdbc.transport.TransportFactory;
import org.opensearch.jdbc.test.WireMockServerHelpers;
import org.opensearch.jdbc.test.mocks.QueryMock;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.get;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(PerTestWireMockServerExtension.class)
public class StatementTests implements WireMockServerHelpers {

    @Test
    void testQueryRequest() throws ResponseException, IOException, SQLException {

        final String sql = "select pickup_datetime, trip_type, passenger_count, " +
                "fare_amount, extra, vendor_id from nyc_taxis LIMIT 5";

        TransportFactory tf = mock(TransportFactory.class);
        ProtocolFactory pf = mock(ProtocolFactory.class);
        Protocol mockProtocol = mock(Protocol.class);

        when(mockProtocol.connect(anyInt())).thenReturn(mock(ConnectionResponse.class));

        when(tf.getTransport(any(), any(), any()))
                .thenReturn(mock(Transport.class));

        when(pf.getProtocol(any(ConnectionConfig.class), any(Transport.class)))
                .thenReturn(mockProtocol);

        when(mockProtocol.execute(any(QueryRequest.class)))
                .thenReturn(mock(QueryResponse.class));

        Connection con = new ConnectionImpl(ConnectionConfig.builder().build(), tf, pf, NoOpLogger.INSTANCE);

        Statement st = con.createStatement();
        ResultSet rs = assertDoesNotThrow(() -> st.executeQuery(sql));

        JdbcQueryRequest request = new JdbcQueryRequest(sql);

        verify(mockProtocol).execute(request);

        // new ResultSetImpl(mock(StatementImpl.class), mock(QueryResponse.class));
        st.close();
        rs.close();
        con.close();
    }


    @Test
    void testEffectiveFetchSizeOnStatement() throws ResponseException, IOException, SQLException {

        TransportFactory tf = mock(TransportFactory.class);
        ProtocolFactory pf = mock(ProtocolFactory.class);
        Protocol mockProtocol = mock(Protocol.class);

        when(mockProtocol.connect(anyInt())).thenReturn(mock(ConnectionResponse.class));

        when(tf.getTransport(any(), any(), any()))
                .thenReturn(mock(Transport.class));

        when(pf.getProtocol(any(ConnectionConfig.class), any(Transport.class)))
                .thenReturn(mockProtocol);

        when(mockProtocol.execute(any(QueryRequest.class)))
                .thenReturn(mock(QueryResponse.class));

        String url = "jdbc:opensearch://localhost:9200?fetchSize=400";

        ConnectionConfig connectionConfig = ConnectionConfig.builder().setUrl(url).build();
        Connection con = new ConnectionImpl(connectionConfig, tf, pf, NoOpLogger.INSTANCE);
        Statement st = con.createStatement();
        assertEquals(st.getFetchSize(), 400);
        st.close();
        con.close();

        // Properties override connection string fetchSize
        Properties properties = new Properties();
        properties.setProperty("fetchSize", "5000");
        connectionConfig = ConnectionConfig.builder().setUrl(url).setProperties(properties).build();
        con = new ConnectionImpl(connectionConfig, tf, pf, NoOpLogger.INSTANCE);
        st = con.createStatement();
        assertEquals(st.getFetchSize(), 5000);
        st.close();
        con.close();


        // setFetchSize overrides fetchSize set anywhere
        connectionConfig = ConnectionConfig.builder().setUrl(url).setProperties(properties).build();
        con = new ConnectionImpl(connectionConfig, tf, pf, NoOpLogger.INSTANCE);
        st = con.createStatement();
        st.setFetchSize(200);
        assertEquals(st.getFetchSize(), 200);
        st.close();
        con.close();

    }

    @Test
    void testQueryInternalServerError(WireMockServer mockServer) throws SQLException, IOException {
        QueryMock queryMock = new QueryMock.NycTaxisQueryInternalErrorMock();

        queryMock.setupMockServerStub(mockServer);

        Connection con = new Driver().connect(getBaseURLForMockServer(mockServer), null);
        Statement st = con.createStatement();
        InternalServerErrorException ex = assertThrows(
                InternalServerErrorException.class, () -> st.executeQuery(queryMock.getSql()));

        String expectedDetails = "java.lang.NullPointerException\n\t" +
                "at java.base/java.lang.Thread.run(Thread.java:844)\n";

        assertEquals("error reason", ex.getReason());
        assertEquals("java.lang.NullPointerException", ex.getType());
        assertEquals(expectedDetails, ex.getDetails());

        st.close();
        con.close();
    }
}

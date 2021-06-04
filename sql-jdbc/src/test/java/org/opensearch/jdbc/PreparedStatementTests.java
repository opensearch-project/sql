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

package org.opensearch.jdbc;

import org.opensearch.jdbc.config.ConnectionConfig;
import org.opensearch.jdbc.logging.NoOpLogger;
import org.opensearch.jdbc.protocol.ConnectionResponse;
import org.opensearch.jdbc.protocol.Protocol;
import org.opensearch.jdbc.protocol.ProtocolFactory;
import org.opensearch.jdbc.protocol.QueryRequest;
import org.opensearch.jdbc.protocol.QueryResponse;
import org.opensearch.jdbc.protocol.exceptions.ResponseException;
import org.opensearch.jdbc.test.PerTestWireMockServerExtension;
import org.opensearch.jdbc.transport.Transport;
import org.opensearch.jdbc.transport.TransportFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * PreparedStatement tests
 *
 * @author echo
 * @since 12.03.20
 **/
@ExtendWith(PerTestWireMockServerExtension.class)
public class PreparedStatementTests {

    final String sql = "select pickup_datetime, trip_type, passenger_count, " +
            "fare_amount, extra, vendor_id from nyc_taxis LIMIT 5";

    private static Stream<Arguments> getArgumentsStream() {
        int[] resultSetTypes = new int[]{ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.TYPE_FORWARD_ONLY};
        int[] resultSetConcurrencyTypes = new int[]{ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};
        int[] resultSetHoldabilityTypes = new int[]{ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT};
        Stream.Builder<Arguments> builder = Stream.builder();
        for (int resultSetType : resultSetTypes) {
            for (int resultSetConcurrencyType : resultSetConcurrencyTypes) {
                for (int resultSetHoldabilityType : resultSetHoldabilityTypes) {
                    builder.add(Arguments.of(resultSetType, resultSetConcurrencyType, resultSetHoldabilityType));
                }
            }
        }
        return builder.build();
    }

    private static Stream<Arguments> resultSetParamsNotSupported() {
        return getArgumentsStream().filter(a -> !Arrays.deepEquals(a.get(), new Object[]{ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT}));
    }

    private static Stream<Arguments> resultSetParamsSupported() {
        return getArgumentsStream().filter(a -> Arrays.deepEquals(a.get(), new Object[]{ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT}));
    }

    private static Stream<Arguments> resultSetParamsNotSupportedHoldability() {
        return getArgumentsStream().filter(a -> !Objects.equals(a.get()[0], ResultSet.TYPE_FORWARD_ONLY) || !Objects.equals(a.get()[1], ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    void testPreparedStatementExecute() throws ResponseException, IOException, SQLException {
        try (Connection con = getMockConnection()) {
            try (PreparedStatement pstm = con.prepareStatement(sql)) {
                assertTrue(pstm.execute());
                ResultSet rs = assertDoesNotThrow(() -> pstm.getResultSet());
                rs.close();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("resultSetParamsNotSupported")
    void testPrepareStatementNotSupported(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException, IOException, ResponseException {
        try (Connection con = getMockConnection()) {
            assertThrows(SQLNonTransientException.class, () -> con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
            assertThrows(SQLNonTransientException.class, () -> con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
        }
    }

    @ParameterizedTest
    @MethodSource("resultSetParamsNotSupportedHoldability")
    void testPrepareStatementNotSupported(int resultSetType, int resultSetConcurrency) throws SQLException, IOException, ResponseException {
        try (Connection con = getMockConnection()) {
            assertThrows(SQLNonTransientException.class, () -> con.prepareStatement(sql, resultSetType, resultSetConcurrency));
            assertThrows(SQLNonTransientException.class, () -> con.createStatement(resultSetType, resultSetConcurrency));
        }
    }

    @ParameterizedTest
    @MethodSource("resultSetParamsSupported")
    void testPrepareStatementSupported(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException, IOException, ResponseException {

        try (Connection con = getMockConnection()) {
            try (PreparedStatement pstm = con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)) {
                assertTrue(pstm.execute());
                ResultSet rs = assertDoesNotThrow(() -> pstm.getResultSet());
                rs.close();
                assertDoesNotThrow(() -> con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).close();
                assertDoesNotThrow(() -> con.prepareStatement(sql, resultSetType, resultSetConcurrency)).close();
                assertDoesNotThrow(() -> con.createStatement(resultSetType, resultSetConcurrency)).close();
            }
        }
    }

    private Connection getMockConnection() throws IOException, ResponseException, SQLException {
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
        return con;
    }

    @Test
    void testEffectiveFetchSizeOnPreparedStatement() throws ResponseException, IOException, SQLException {

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
        PreparedStatement st = con.prepareStatement(sql);
        assertEquals(st.getFetchSize(), 400);
        st.close();
        con.close();

        // Properties override connection string fetchSize
        Properties properties = new Properties();
        properties.setProperty("fetchSize", "5000");
        connectionConfig = ConnectionConfig.builder().setUrl(url).setProperties(properties).build();
        con = new ConnectionImpl(connectionConfig, tf, pf, NoOpLogger.INSTANCE);
        st = con.prepareStatement(sql);
        assertEquals(st.getFetchSize(), 5000);
        st.close();
        con.close();


        // setFetchSize overrides fetchSize set anywhere
        connectionConfig = ConnectionConfig.builder().setUrl(url).setProperties(properties).build();
        con = new ConnectionImpl(connectionConfig, tf, pf, NoOpLogger.INSTANCE);
        st = con.prepareStatement(sql);
        st.setFetchSize(200);
        assertEquals(st.getFetchSize(), 200);
        st.close();
        con.close();

    }

}

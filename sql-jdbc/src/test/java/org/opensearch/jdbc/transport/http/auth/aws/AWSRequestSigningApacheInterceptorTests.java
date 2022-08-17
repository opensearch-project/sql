/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.transport.http.auth.aws;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.Signer;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AWSRequestSigningApacheInterceptorTests {

    @Test
    public void testSimpleSigner() throws Exception {
        HttpEntityEnclosingRequest request =
                new BasicHttpEntityEnclosingRequest("GET", "/query?a=b");
        request.setEntity(new StringEntity("I'm an entity"));
        request.addHeader("foo", "bar");
        request.addHeader("content-length", "0");

        HttpCoreContext context = new HttpCoreContext();
        context.setTargetHost(HttpHost.create("localhost"));

        createInterceptor().process(request, context);

        assertEquals("bar", request.getFirstHeader("foo").getValue());
        assertEquals("wuzzle", request.getFirstHeader("Signature").getValue());
        assertNull(request.getFirstHeader("content-length"));
    }

    @Test
    public void testBadRequest() throws Exception {

        HttpRequest badRequest = new BasicHttpRequest("GET", "?#!@*%");
        assertThrows(IOException.class,
                () -> createInterceptor().process(badRequest, new BasicHttpContext()));
    }

    private static class AddHeaderSigner implements Signer {
        private final String name;
        private final String value;

        private AddHeaderSigner(String name, String value) {
            this.name = name;
            this.value = value;
        }


        @Override
        public void sign(SignableRequest<?> request, AWSCredentials credentials) {
            request.addHeader(name, value);
            request.addHeader("resourcePath", request.getResourcePath());
        }
    }

    @Test
    public void testEncodedUriSigner() throws Exception {
        HttpEntityEnclosingRequest request =
                new BasicHttpEntityEnclosingRequest("GET", "/foo-2017-02-25%2Cfoo-2017-02-26/_search?a=b");
        request.setEntity(new StringEntity("I'm an entity"));
        request.addHeader("foo", "bar");
        request.addHeader("content-length", "0");

        HttpCoreContext context = new HttpCoreContext();
        context.setTargetHost(HttpHost.create("localhost"));

        createInterceptor().process(request, context);

        assertEquals("bar", request.getFirstHeader("foo").getValue());
        assertEquals("wuzzle", request.getFirstHeader("Signature").getValue());
        assertNull(request.getFirstHeader("content-length"));
        assertEquals("/foo-2017-02-25%2Cfoo-2017-02-26/_search", request.getFirstHeader("resourcePath").getValue());
    }

    private static AWSRequestSigningApacheInterceptor createInterceptor() {
        AWSCredentialsProvider anonymousCredentialsProvider =
                new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
        return new AWSRequestSigningApacheInterceptor("servicename",
                new AddHeaderSigner("Signature", "wuzzle"),
                anonymousCredentialsProvider);

    }
}

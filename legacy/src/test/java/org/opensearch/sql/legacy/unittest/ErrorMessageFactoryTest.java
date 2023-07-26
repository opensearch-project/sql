/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.legacy.executor.format.ErrorMessage;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.executor.format.OpenSearchErrorMessage;

public class ErrorMessageFactoryTest {

    private Throwable nonOpenSearchThrowable = new Throwable();
    private Throwable openSearchThrowable = new OpenSearchException(nonOpenSearchThrowable);

    @Test
    public void openSearchExceptionShouldCreateEsErrorMessage() {
        Exception exception = new OpenSearchException(nonOpenSearchThrowable);
        ErrorMessage msg = ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
        Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
    }

    @Test
    public void nonOpenSearchExceptionShouldCreateGenericErrorMessage() {
        Exception exception = new Exception(nonOpenSearchThrowable);
        ErrorMessage msg = ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
        Assert.assertFalse(msg instanceof OpenSearchErrorMessage);
    }

    @Test
    public void nonOpenSearchExceptionWithWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
        Exception exception = (Exception) openSearchThrowable;
        ErrorMessage msg = ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
        Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
    }

    @Test
    public void nonOpenSearchExceptionWithMultiLayerWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
        Exception exception = new Exception(new Throwable(new Throwable(openSearchThrowable)));
        ErrorMessage msg = ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
        Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
    }

}

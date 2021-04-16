/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.rest.RestStatus;

public class ErrorMessageFactoryTest {
  private Throwable nonOpenSearchThrowable = new Throwable();
  private Throwable openSearchThrowable = new OpenSearchException(nonOpenSearchThrowable);

  @Test
  public void openSearchExceptionShouldCreateEsErrorMessage() {
    Exception exception = new OpenSearchException(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionShouldCreateGenericErrorMessage() {
    Exception exception = new Exception(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertFalse(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionWithWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = (Exception) openSearchThrowable;
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void
      nonOpenSearchExceptionWithMultiLayerWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = new Exception(new Throwable(new Throwable(openSearchThrowable)));
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }
}

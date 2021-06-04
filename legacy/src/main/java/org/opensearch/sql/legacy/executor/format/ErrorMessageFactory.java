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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.executor.format;

import org.opensearch.OpenSearchException;

public class ErrorMessageFactory {
    /**
     * Create error message based on the exception type
     * Exceptions of OpenSearch exception type and exceptions with wrapped OpenSearch exception causes
     * should create {@link OpenSearchErrorMessage}
     *
     * @param e         exception to create error message
     * @param status    exception status code
     * @return          error message
     */

    public static ErrorMessage createErrorMessage(Exception e, int status) {
        if (e instanceof OpenSearchException) {
            return new OpenSearchErrorMessage((OpenSearchException) e,
                    ((OpenSearchException) e).status().getStatus());
        } else if (unwrapCause(e) instanceof OpenSearchException) {
            OpenSearchException exception = (OpenSearchException) unwrapCause(e);
            return new OpenSearchErrorMessage(exception, exception.status().getStatus());
        }
        return new ErrorMessage(e, status);
    }

    public static Throwable unwrapCause(Throwable t) {
        Throwable result = t;
        if (result instanceof OpenSearchException) {
            return result;
        }
        if (result.getCause() == null) {
            return result;
        }
        result = unwrapCause(result.getCause());
        return result;
    }
}

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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.request;

import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.legacy.executor.Format;

/**
 * Utils class for parse the request params.
 */
public class SqlRequestParam {
    public static final String QUERY_PARAMS_FORMAT = "format";
    public static final String QUERY_PARAMS_PRETTY = "pretty";
    public static final String QUERY_PARAMS_ESCAPE = "escape";

    private static final String DEFAULT_RESPONSE_FORMAT = "jdbc";

    /**
     * Parse the pretty params to decide whether the response should be pretty formatted.
     * @param requestParams request params.
     * @return return true if the response required pretty format, otherwise return false.
     */
    public static boolean isPrettyFormat(Map<String, String> requestParams) {
        return requestParams.containsKey(QUERY_PARAMS_PRETTY)
               && ("".equals(requestParams.get(QUERY_PARAMS_PRETTY))
                   || "true".equals(requestParams.get(QUERY_PARAMS_PRETTY)));
    }

    /**
     * Parse the request params and return the {@link Format} of the response
     * @param requestParams request params
     * @return The response Format.
     */
    public static Format getFormat(Map<String, String> requestParams) {
        String formatName =
            requestParams.containsKey(QUERY_PARAMS_FORMAT)
                ? requestParams.get(QUERY_PARAMS_FORMAT).toLowerCase()
                : DEFAULT_RESPONSE_FORMAT;
        Optional<Format> optionalFormat = Format.of(formatName);
        if (optionalFormat.isPresent()) {
            return optionalFormat.get();
        } else {
            throw new IllegalArgumentException("Failed to create executor due to unknown response format: "
                                               + formatName);
        }
    }

    public static boolean getEscapeOption(Map<String, String> requestParams) {
        if (requestParams.containsKey(QUERY_PARAMS_ESCAPE)) {
            return Boolean.parseBoolean(requestParams.get(QUERY_PARAMS_ESCAPE));
        }
        return false;
    }
}

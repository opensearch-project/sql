/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.utils;

import java.io.IOException;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

/**
 * Utility Class for formatting Json string pretty.
 */
public class JsonPrettyFormatter {

    /**
     * @param jsonString Json string without/with pretty format
     * @return A standard and pretty formatted json string
     * @throws IOException
     */
    public static String format(String jsonString) throws IOException {
        //turn _explain response into pretty formatted Json
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().prettyPrint();
        try (
                XContentParser contentParser = XContentFactory.xContent(XContentType.JSON)
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, jsonString)
        ){
            contentBuilder.copyCurrentStructure(contentParser);
        }
        return Strings.toString(contentBuilder);
    }

    private JsonPrettyFormatter() {
        throw new AssertionError(getClass().getCanonicalName() + " is a utility class and must not be initialized");
    }
}

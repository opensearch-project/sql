/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods to work with AWS format hostnames
 */
public class AwsHostNameUtil {

    private static final Pattern REGION_PATTERN =
            Pattern.compile("^(?:.+\\.)?([a-z0-9-]+)$");

    private static final Pattern KNOWN_HOSTNAME_PATTERN =
            Pattern.compile("^(?:.+)?(\\.es\\.[a-z0-9]+\\.com)$");

    /**
     * Returns the region name contained in a specified endpoint based
     * on known conventions for endpoint formats.
     *
     * @param hostname the hostname to parse
     *
     * @return the region parsed from the hostname, or
     *         null if region could not be determined.
     */
    public static String parseRegion(final String hostname) {
        if (hostname == null) {
            throw new IllegalArgumentException("hostname cannot be null");
        }

        String region = null;
        int knownSuffixLength = 0;

        Matcher matcher = KNOWN_HOSTNAME_PATTERN.matcher(hostname);
        if (matcher.matches()) {
            knownSuffixLength = matcher.group(1).length();
        }

        if (knownSuffixLength > 0) {
            // hostname has the format 'ABC.es.XYZ.com'
            int index = hostname.length() - knownSuffixLength;
            region = parseStandardRegionName(hostname.substring(0, index));
        }

        return region;
    }

    /**
     * Parses the region name from an endpoint fragment.
     *
     * @param fragment the portion of the endpoint up to the region name
     *
     * @return the parsed region name (or null if we can't tell for sure)
     */
    private static String parseStandardRegionName(final String fragment) {
        Matcher matcher = REGION_PATTERN.matcher(fragment);
        if (matcher.matches()) {
            // fragment is of the form 'domain-name.region'
            // return the region component
            return matcher.group(1);
        } else {
            return null;
        }
    }

}

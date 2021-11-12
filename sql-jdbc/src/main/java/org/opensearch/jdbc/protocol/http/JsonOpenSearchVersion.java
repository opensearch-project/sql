/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.OpenSearchVersion;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonOpenSearchVersion implements OpenSearchVersion {

    private String fullVersion;
    private int[] version = new int[3];

    public JsonOpenSearchVersion(@JsonProperty("number") String fullVersion) {
        if (fullVersion == null)
            return;

        this.fullVersion = fullVersion;
        String[] versionTokens = fullVersion.split("[.-]");

        for (int i=0; i<versionTokens.length && i < 3; i++) {
            this.version[i] = parseNumber(versionTokens[i]);
        }
    }

    private int parseNumber(String str) {
        int number = 0;
        try {
            number = Integer.parseInt(str);
        } catch (NumberFormatException nfe) {
            // eat
        }
        return number;
    }

    @Override
    public int getMajor() {
        return version[0];
    }

    @Override
    public int getMinor() {
        return version[1];
    }

    @Override
    public int getRevision() {
        return version[2];
    }

    @Override
    public String getFullVersion() {
        return fullVersion;
    }
}

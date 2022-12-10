/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal;

public enum Version {

    // keep this in sync with the sql-jdbc/build.gradle file
    Current(2, 0, 0, 0);

    private int major;
    private int minor;
    private int build;
    private int revision;

    private String fullVersion;

    Version(int major, int minor, int build, int revision) {
        this.major = major;
        this.minor = minor;
        this.build = build;
        this.revision = revision;
        this.fullVersion = String.format("%d.%d.%d.%d", major, minor, build, revision);
    }

    public int getMajor() {
        return this.major;
    }

    public int getMinor() {
        return this.minor;
    }

    public int getBuild() {
        return this.build;
    }

    public int getRevision() {
        return this.revision;
    }

    public String getFullVersion() {
        return this.fullVersion;
    }
}

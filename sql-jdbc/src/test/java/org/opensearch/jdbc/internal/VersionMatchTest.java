/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.junit.jupiter.api.Test;
import org.opensearch.jdbc.internal.Version;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VersionMatchTest {

    @Test
    void testVersionMatchesBuildGradleVersion() {
        assertEquals(Version.Current.getFullVersion(), System.getProperty("opensearch_jdbc_version"));
    }
}

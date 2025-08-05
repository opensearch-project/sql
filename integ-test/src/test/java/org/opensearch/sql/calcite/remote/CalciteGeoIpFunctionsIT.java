/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.GeoIpFunctionsIT;

import java.io.IOException;

public class CalciteGeoIpFunctionsIT extends GeoIpFunctionsIT {
    @Override
    public void init() throws IOException {
        super.init();
        enableCalcite();
    }
}
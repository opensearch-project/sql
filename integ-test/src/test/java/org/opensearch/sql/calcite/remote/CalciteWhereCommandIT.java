/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.Ignore;
import org.opensearch.sql.ppl.WhereCommandIT;

import java.io.IOException;

@Ignore("Not all boolean functions are supported in Calcite now")
public class CalciteWhereCommandIT extends WhereCommandIT {
    @Override
    public void init() throws IOException {
        enableCalcite();
        disallowCalciteFallback();
        super.init();
    }
}

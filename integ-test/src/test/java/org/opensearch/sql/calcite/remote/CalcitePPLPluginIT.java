package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.PPLPluginIT;

public class CalcitePPLPluginIT extends PPLPluginIT {
  @Override
  public void init() throws Exception {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}

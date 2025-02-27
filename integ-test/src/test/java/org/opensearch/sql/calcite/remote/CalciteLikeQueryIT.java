/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.LikeQueryIT;

//@Ignore("CalciteLikeQueryIT is not supported in OpenSearch yet")
public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Ignore("* in like is handled wrong")
  @Override
  public void test_like_on_text_field_with_one_word() throws IOException {
    super.test_like_on_text_field_with_one_word();
  }

  @Ignore("* in like is handled wrong")
  @Override
  public void test_like_on_text_keyword_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_keyword_field_with_greater_than_one_word();
  }

  @Ignore("* in like is handled wrong")
  @Override
  public void test_like_on_text_field_with_greater_than_one_word() throws IOException {
    super.test_like_on_text_field_with_greater_than_one_word();
  }

  @Ignore("* in like is handled wrong")
  @Override
  public void test_convert_field_text_to_keyword() throws IOException {
    super.test_convert_field_text_to_keyword();
  }

}

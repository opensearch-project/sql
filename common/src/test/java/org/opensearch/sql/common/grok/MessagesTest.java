/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.common.grok;

import static org.junit.Assert.assertNotNull;

import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.opensearch.sql.common.grok.exception.GrokException;


public class MessagesTest {

  @Test
  public void test001_linux_messages() throws GrokException, IOException {
    GrokCompiler compiler = GrokCompiler.newInstance();
    compiler.register(Resources.getResource(ResourceManager.PATTERNS).openStream());

    Grok grok = compiler.compile("%{MESSAGESLOG}");

    BufferedReader br = new BufferedReader(
        new FileReader(Resources.getResource(ResourceManager.MESSAGES).getFile()));
    String line;
    System.out.println("Starting test with linux messages log -- may take a while");
    while ((line = br.readLine()) != null) {
      Match gm = grok.match(line);
      Map<String, Object> map = gm.capture();
      assertNotNull(map);
      Assertions.assertThat(map).doesNotContainKey("Error");
    }
    br.close();
  }

}

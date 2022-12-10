/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.grok;


import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.common.grok.exception.GrokException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ApacheDataTypeTest {

  static {
    Locale.setDefault(Locale.ROOT);
  }

  private final String line =
      "64.242.88.10 - - [07/Mar/2004:16:45:56 -0800] \"GET /twiki/bin/attach/Main/PostfixCommands "
          + "HTTP/1.1\" 401 12846";

  private GrokCompiler compiler;

  @Before
  public void setup() throws Exception {
    compiler = GrokCompiler.newInstance();
    compiler.register(Resources.getResource(ResourceManager.PATTERNS).openStream());
  }

  @Test
  public void test002_httpd_access_semi() throws GrokException {
    Grok grok = compiler.compile(
        "%{IPORHOST:clientip} %{USER:ident;boolean} %{USER:auth} "
            + "\\[%{HTTPDATE:timestamp;date;dd/MMM/yyyy:HH:mm:ss Z}\\] \"(?:%{WORD:verb;string} "
            + "%{NOTSPACE:request}"
            + "(?: HTTP/%{NUMBER:httpversion;float})?|%{DATA:rawrequest})\" %{NUMBER:response;int} "
            + "(?:%{NUMBER:bytes;long}|-)");

    System.out.println(line);
    Match gm = grok.match(line);
    Map<String, Object> map = gm.capture();

    Assertions.assertThat(map).doesNotContainKey("Error");
    Instant ts = ZonedDateTime.of(2004, 3, 7, 16, 45, 56, 0, ZoneOffset.ofHours(-8)).toInstant();
    assertEquals(map.get("timestamp"), ts);
    assertEquals(map.get("response"), 401);
    assertEquals(map.get("ident"), Boolean.FALSE);
    assertEquals(map.get("httpversion"), 1.1f);
    assertEquals(map.get("bytes"), 12846L);
    assertEquals("GET", map.get("verb"));

  }

  @Test
  public void test002_httpd_access_colon() throws GrokException {
    Grok grok = compiler.compile(
        "%{IPORHOST:clientip} %{USER:ident:boolean} %{USER:auth} "
            + "\\[%{HTTPDATE:timestamp:date:dd/MMM/yyyy:HH:mm:ss Z}\\] \"(?:%{WORD:verb:string} "
            + "%{NOTSPACE:request}"
            + "(?: HTTP/%{NUMBER:httpversion:float})?|%{DATA:rawrequest})\" %{NUMBER:response:int} "
            + "(?:%{NUMBER:bytes:long}|-)");

    Match gm = grok.match(line);
    Map<String, Object> map = gm.capture();

    Assertions.assertThat(map).doesNotContainKey("Error");

    Instant ts = ZonedDateTime.of(2004, 3, 7, 16, 45, 56, 0, ZoneOffset.ofHours(-8)).toInstant();
    assertEquals(map.get("timestamp"), ts);
    assertEquals(map.get("response"), 401);
    assertEquals(map.get("ident"), Boolean.FALSE);
    assertEquals(map.get("httpversion"), 1.1f);
    assertEquals(map.get("bytes"), 12846L);
    assertEquals("GET", map.get("verb"));

  }
}

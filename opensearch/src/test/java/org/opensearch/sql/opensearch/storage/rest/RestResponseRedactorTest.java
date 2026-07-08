/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RestResponseRedactorTest {

  @Test
  void masksIpv4() {
    assertEquals("x.x.x.x", RestResponseRedactor.redact("0.0.0.0"));
    assertEquals("x.x.x.x", RestResponseRedactor.redact("255.255.255.255"));
    assertEquals("x.x.x.x", RestResponseRedactor.redact("192.168.1.1"));
    assertEquals("x.x.x.x", RestResponseRedactor.redact("1.2.3.4"));
    assertEquals("x.x.x.x:9200", RestResponseRedactor.redact("10.0.0.1:9200"));
    assertEquals("a x.x.x.x b x.x.x.x c", RestResponseRedactor.redact("a 10.0.0.1 b 172.16.5.4 c"));
  }

  @Test
  void doesNotMaskInvalidOrPartialIpv4() {
    assertEquals("256.1.1.1", RestResponseRedactor.redact("256.1.1.1"));
    assertEquals("1.2.3", RestResponseRedactor.redact("1.2.3"));
    assertEquals("44", RestResponseRedactor.redact("44"));
  }

  @Test
  void masksEc2HostName() {
    assertEquals("<host>", RestResponseRedactor.redact("ip-10-0-0-1"));
    assertEquals("<host>", RestResponseRedactor.redact("ip-172-31-255-9"));
    assertEquals("node <host> here", RestResponseRedactor.redact("node ip-10-1-2-3 here"));
    assertEquals("ip-256-0-0-1", RestResponseRedactor.redact("ip-256-0-0-1"));
  }

  @Test
  void masksFullIpv6() {
    assertEquals("x.x.x.x", RestResponseRedactor.redact("fe80:0:0:0:0:0:0:1"));
    assertEquals("x.x.x.x", RestResponseRedactor.redact("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
    assertEquals("x.x.x.x", RestResponseRedactor.redact("FE80:0:0:0:0:0:0:1"));
  }

  @Test
  void doesNotMaskCompressedIpv6() {
    assertEquals("::1", RestResponseRedactor.redact("::1"));
    assertEquals("2001:db8::1", RestResponseRedactor.redact("2001:db8::1"));
  }

  @Test
  void masksInetAddress() {
    assertEquals("inet[/x.x.x.x:9200]", RestResponseRedactor.redact("inet[/10.0.0.7:9200]"));
  }

  @Test
  void masksAvailabilityZones() {
    assertEquals("xx-xxxxx-xx", RestResponseRedactor.redact("us-east-1a"));
    assertEquals("xx-xxxxx-xx", RestResponseRedactor.redact("ap-southeast-2b"));
    assertEquals("xx-xxxxx-xx", RestResponseRedactor.redact("eu-west-1c"));
    assertEquals("xx-xxxxx-xx", RestResponseRedactor.redact("us-gov-west-1a"));
    assertEquals(
        "a xx-xxxxx-xx b xx-xxxxx-xx", RestResponseRedactor.redact("a us-east-1a b us-west-2b"));
  }

  @Test
  void maskAvailabilityZoneMasksOnlyZones() {
    assertEquals("xx-xxxxx-xx", RestResponseRedactor.maskAvailabilityZone("us-east-1a"));
    assertEquals("10.0.0.7", RestResponseRedactor.maskAvailabilityZone("10.0.0.7"));
    assertEquals("ip-10-0-0-1", RestResponseRedactor.maskAvailabilityZone("ip-10-0-0-1"));
  }

  @Test
  void leavesNonAddressesIntact() {
    assertEquals(
        "e4e136ea81e27370ff73cf753ba22d39",
        RestResponseRedactor.redact("e4e136ea81e27370ff73cf753ba22d39"));
    assertEquals(
        "data,ingest,remote_cluster_client",
        RestResponseRedactor.redact("data,ingest,remote_cluster_client"));
    assertEquals(
        "x.x.x.x 44 95 imr - e4e136ea",
        RestResponseRedactor.redact("10.0.0.7 44 95 imr - e4e136ea"));
  }

  @Test
  void handlesNullAndEmpty() {
    assertEquals(null, RestResponseRedactor.redact(null));
    assertEquals("", RestResponseRedactor.redact(""));
    assertEquals(null, RestResponseRedactor.maskAvailabilityZone(null));
    assertEquals("", RestResponseRedactor.maskAvailabilityZone(""));
  }
}

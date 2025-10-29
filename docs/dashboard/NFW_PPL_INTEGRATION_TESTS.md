# Network Firewall PPL Integration Tests

## Overview

This document describes the integration tests for Network Firewall (NFW) PPL dashboard queries in OpenSearch. These tests ensure that NFW-related PPL queries work correctly with actual AWS Network Firewall log data.

## Test Class

**File**: `NfwPplDashboardIT.java`  
**Location**: `/integ-test/src/test/java/org/opensearch/sql/ppl/dashboard/`  
**Test Data**: `/integ-test/src/test/resources/nfw_logs.json`  
**Index Mapping**: `/integ-test/src/test/resources/indexDefinitions/nfw_logs_index_mapping.json`

## Test Coverage

The NFW dashboard tests cover 37 comprehensive dashboard scenarios:

### 1. Top Application Protocols (`testTopApplicationProtocols`)
```sql
source=nfw_logs | where isnotnull(`event.app_proto`) | STATS count() as Count by `event.app_proto` | SORT - Count| HEAD 10
```
- **Purpose**: Shows most common application layer protocols
- **Expected**: unknown (5), http (3), tls (2), dns (2)

### 2. Top Source IP by Packets (`testTopSourceIPByPackets`)
```sql
source=nfw_logs | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`, 2d) as timestamp_span, `event.src_ip` | rename `event.src_ip` as `Source IP` | sort - packet_count | head 10
```
- **Purpose**: Identifies source IPs generating the most network packets over time
- **Expected**: 10.170.18.235 with 53 packets

### 3. Top Source IP by Bytes (`testTopSourceIPByBytes`)
```sql
source=nfw_logs | stats sum(`event.netflow.bytes`) as sum_bytes by span(`event.timestamp`, 2d) as timestamp_span, `event.src_ip` | rename  `event.src_ip` as `Source IP` | sort - sum_bytes | head 10
```
- **Purpose**: Identifies source IPs generating the most network traffic by bytes over time
- **Expected**: 10.170.18.235 with 4142 bytes

### 4. Top Destination IP by Packets (`testTopDestinationIPByPackets`)
```sql
source=nfw_logs | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`, 2d) as timestamp_span, `event.dest_ip` | rename `event.dest_ip` as `Destination IP` | sort - packet_count | head 10
```
- **Purpose**: Identifies destination IPs receiving the most packets over time
- **Expected**: 8.8.8.8 with 31 packets

### 4. Top Protocols (`testTopProtocols`)
```sql
source=nfw_logs | stats count() as Protocol by `event.proto` | sort - Protocol | head 1
```
- **Purpose**: Shows most common network protocols
- **Expected**: TCP with 10 occurrences

### 5. Top Application Protocols (`testTopApplicationProtocols`)
```sql
source=nfw_logs | stats count() as Protocol by `event.app_proto` | sort - Protocol | head 1
```
- **Purpose**: Shows most common application layer protocols
- **Expected**: unknown with 10 occurrences

### 6. Top Source Ports (`testTopSourcePorts`)
```sql
source=nfw_logs | stats count() as Count by `event.src_port` | sort - Count | head 1
```
- **Purpose**: Identifies most common source ports
- **Expected**: Port 37334 with 10 occurrences

### 7. Top Destination Ports (`testTopDestinationPorts`)
```sql
source=nfw_logs | stats count() as Count by `event.dest_port` | sort - Count | head 3
```
- **Purpose**: Identifies most common destination ports
- **Expected**: Various ports (4663, 7655, 11703) with 1 occurrence each

### 8. Top TCP Flags (`testTopTCPFlags`)
```sql
source=nfw_logs | stats count() as Count by `event.tcp.tcp_flags` | sort - Count | head 1
```
- **Purpose**: Shows distribution of TCP flags
- **Expected**: Flag "02" (SYN) with 10 occurrences

### 9. Top Flow IDs (`testTopFlowIDs`)
```sql
source=nfw_logs | stats count() as Count by `event.flow_id` | sort - Count | head 3
```
- **Purpose**: Shows flow ID distribution for connection tracking
- **Expected**: Unique flow IDs with 1 occurrence each

### 10. Top TCP Flows (`testTopTCPFlows`)
```sql
source=nfw_logs | where `event.proto` = 'TCP' | stats count() as Count by `event.src_ip`, `event.dest_ip` | sort - Count | head 1
```
- **Purpose**: Identifies most common TCP connections between source and destination
- **Expected**: 3.80.106.210 → 10.2.1.120 with 10 flows

### 11. Top Long-Lived TCP Flows (`testTopLongLivedTCPFlows`)
```sql
source=nfw_logs | WHERE `event.proto` = 'TCP' and `event.netflow.age` > 350 | STATS count() as Count by SPAN(`event.timestamp`, 2d), `event.src_ip`, `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst IP:Port` = CONCAT(`event.src_ip`, ": ", CAST(`event.src_port` AS STRING), " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10
```
- **Purpose**: Identifies TCP connections that have been active for more than 350 seconds
- **Expected**: Long-lived TCP flows with formatted source and destination information

### 12-37. Additional Comprehensive Tests
The remaining 26 tests cover:
- **Destination IP by Bytes** - Traffic volume analysis for destinations
- **Source-Destination Packet/Byte Analysis** - Combined flow analysis
- **TCP Flow Analysis by Packets/Bytes** - Detailed TCP connection metrics
- **Combined Packet and Byte Metrics** - Multi-dimensional traffic analysis
- **Infrastructure Analysis** - Firewall name and availability zone distribution
- **Event Type Analysis** - Netflow event categorization
- **TCP Flag Analysis** - SYN flag detection and analysis
- **Flow Characteristics** - Age and TTL analysis for network optimization

## Data Structure

### NFW Log Format (without aws.networkfirewall prefix)

The test data uses the real AWS Network Firewall log structure:

```json
{
  "firewall_name": "NetworkFirewallSetup-firewall",
  "availability_zone": "us-east-1a", 
  "event_timestamp": "1742422274",
  "event": {
    "src_ip": "3.80.106.210",
    "dest_ip": "10.2.1.120",
    "src_port": 37334,
    "dest_port": 7655,
    "proto": "TCP",
    "app_proto": "unknown",
    "event_type": "netflow",
    "flow_id": 363840402826442,
    "timestamp": "2025-03-19T22:11:14.249819+0000",
    "netflow": {
      "pkts": 1,
      "bytes": 44,
      "start": "2025-03-19T22:05:21.412393+0000",
      "end": "2025-03-19T22:05:21.412393+0000",
      "age": 0,
      "min_ttl": 56,
      "max_ttl": 56
    },
    "tcp": {
      "tcp_flags": "02",
      "syn": true
    }
  }
}
```

### Key Field Mappings

| Dashboard Field | Test Field | Type | Description |
|----------------|------------|------|-------------|
| Source IP | `event.src_ip` | keyword | Source IP address |
| Destination IP | `event.dest_ip` | keyword | Destination IP address |
| Source Port | `event.src_port` | integer | Source port number |
| Destination Port | `event.dest_port` | integer | Destination port number |
| Protocol | `event.proto` | keyword | Network protocol (TCP, UDP, ICMP) |
| App Protocol | `event.app_proto` | keyword | Application protocol |
| Packets | `event.netflow.pkts` | integer | Packet count |
| Bytes | `event.netflow.bytes` | integer | Byte count |
| TCP Flags | `event.tcp.tcp_flags` | keyword | TCP flag values |
| Flow ID | `event.flow_id` | long | Unique flow identifier |

## Test Data

The test uses 10 realistic NFW log records with:
- **Single Source IP**: 3.80.106.210 (external)
- **Single Destination IP**: 10.2.1.120 (internal)
- **Single Source Port**: 37334
- **Various Destination Ports**: 4663, 7655, 11703, etc.
- **Protocol**: All TCP traffic
- **TCP Flags**: All SYN packets (flag "02")
- **Consistent Packet/Byte Counts**: 1 packet, 44 bytes per flow

## Running NFW Tests

```bash
# Run all NFW dashboard tests
./gradlew :integ-test:test --tests "*NfwPplDashboardIT*"

# Run specific NFW test
./gradlew :integ-test:test --tests "*NfwPplDashboardIT.testTopSourceIPByPackets"
```

## Field Syntax

All NFW queries use clean field syntax without AWS prefixes:

- ✅ **Correct**: `event.src_ip`, `event.netflow.pkts`
- ❌ **Incorrect**: `aws.networkfirewall.event.src_ip`, `aws.networkfirewall.event.netflow.pkts`

This provides cleaner, more readable dashboard queries while maintaining full compatibility with AWS Network Firewall log structure.
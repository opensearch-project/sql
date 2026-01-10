# IP Address Functions  

## CIDRMATCH  

### Description  

Usage: `cidrmatch(ip, cidr)` checks if `ip` is within the specified `cidr` range.

**Argument type:** `STRING`/`IP`, `STRING`  
**Return type:** `BOOLEAN`  

### Example
  
```ppl
source=weblogs
| where cidrmatch(host, '1.2.3.0/24')
| fields host, url
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+---------+--------------------+
| host    | url                |
|---------+--------------------|
| 1.2.3.4 | /history/voyager1/ |
| 1.2.3.5 | /history/voyager2/ |
+---------+--------------------+
```
  
Note:
 - `ip` can be an IPv4 or IPv6 address  
 - `cidr` can be an IPv4 or IPv6 block  
 - `ip` and `cidr` must both be valid and non-missing/non-null  
  
## GEOIP  

### Description  

Usage: `geoip(dataSourceName, ipAddress[, options])` to lookup location information from given IP addresses via OpenSearch GeoSpatial plugin API.

**Argument type:** `STRING`, `STRING`/`IP`, `STRING`  
**Return type:** `OBJECT`  

### Example:
  
```ppl ignore
source=weblogs 
| eval LookupResult = geoip("dataSourceName", "50.68.18.229", "country_iso_code,city_name")
```
  
```text
fetched rows / total rows = 1/1
+-------------------------------------------------------------+
| LookupResult                                                |
|-------------------------------------------------------------|
| {'city_name': 'Vancouver', 'country_iso_code': 'CA'}        |
+-------------------------------------------------------------+
```
  
Note:
 - `dataSourceName` must be an established dataSource on OpenSearch GeoSpatial plugin, detail of configuration can be found: https://opensearch.org/docs/latest/ingest-pipelines/processors/ip2geo/  
 - `ip` can be an IPv4 or an IPv6 address  
 - `options` is an optional String of comma separated fields to output: the selection of fields is subject to dataSourceProvider's schema.  For example, the list of fields in the provided `geolite2-city` dataset includes: "country_iso_code", "country_name", "continent_name", "region_iso_code", "region_name", "city_name", "time_zone", "location"  

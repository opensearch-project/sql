====================
Geo IP Address Functions
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

GEOIP
---------

Description
>>>>>>>>>>>

Usage: `geoip(dataSourceName, ipAddress[, options])` to lookup location information from given IP addresses via OpenSearch GeoSpatial plugin API.

Argument type: STRING, STRING, STRING

Return type: OBJECT

Example:

    os> source=weblogs | eval LookupResult = geoip("dataSourceName", "50.68.18.229", "country_iso_code,city_name")
    fetched rows / total rows = 1/1
    +-------------------------------------------------------------+
    | LookupResult                                                        |
    |-------------------------------------------------------------|
    | {'city_name': 'Vancouver', 'country_iso_code': 'CA'}        |
    +-------------------------------------------------------------+


Note:
 - `dataSourceName` must be an established dataSource on OpenSearch GeoSpatial plugin, detail of configuration can be found: https://opensearch.org/docs/latest/ingest-pipelines/processors/ip2geo/
 - `ip` can be an IPv4 or an IPv6 address
 - `options` is a comma separated String of fields to output: the selection of fields is subject to dataSourceProvider's schema.  For example, the list of fields in the provided `geolite2-city` dataset includes: "country_iso_code", "country_name", "continent_name", "region_iso_code", "region_name", "city_name", "time_zone", "location"


# Monitoring  

## Introduction  

By a stats endpoint, you are able to collect metrics for the plugin within the interval. Note that only node level statistics collecting is implemented for now. In other words, you only get the metrics for the node you're accessing. Cluster level statistics have yet to be implemented.  

## Node Stats  

### Description  

The meaning of fields in the response is as follows:  

| Field name | Description |
|------------|-------------|
| `ppl_request_total` | Total count of PPL request |
| `ppl_request_count` | Total count of PPL request within the interval |
| `ppl_failed_request_count_syserr` | Count of failed PPL request due to system error within the interval |
| `ppl_failed_request_count_cuserr` | Count of failed PPL request due to bad request within the interval |  

### Example  

```bash ignore
curl -H 'Content-Type: application/json' -X GET localhost:9200/_plugins/_ppl/stats
```

```json
{
  "ppl_request_total": 10,
  "ppl_request_count": 2,
  "ppl_failed_request_count_syserr": 0,
  "ppl_failed_request_count_cuserr": 0,
  ...
}
```
  
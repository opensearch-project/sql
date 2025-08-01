---
name: 🐛 PPL Bug Report
about: Report a bug or unexpected behavior in PPL
title: '[BUG] Brief description of the issue'
labels: 'bug, untriaged, PPL'
assignees: ''
---
## Query Information
**PPL Command/Query:**
```ppl
# Paste your PPL command or query here
# Remember to sanitize any sensitive fields or values
```
**Expected Result:**
<!-- Describe what you expected to happen -->

**Actual Result:**
<!-- Describe what actually happened, including any error messages -->
<!-- Make sure to redact any sensitive information in error messages -->

## Dataset Information
**Dataset/Schema Type**
- [x] OpenTelemetry (OTEL)
- [ ] Simple Schema for Observability (SS4O)
- [ ] Open Cybersecurity Schema Framework (OCSF)
- [ ] Custom (details below)

**Index Mapping**
```json
{
  "mappings": {
    "properties": {
      "field_name": { "type": "type" }
      // Add your index mapping here
      // Replace sensitive field names with generic alternatives
    }
  }
}
```
**Sample Data**
```json
{
  // Add sample document that reproduces the issue
  // Use dummy/anonymized data for sensitive fields
  // Example: Replace real IPs with 10.0.0.x, real emails with user@example.com
}
```

## Bug Description
**Issue Summary:**
<!-- A clear and concise description of what the bug is -->

**Steps to Reproduce:**
1.
2.
3.

**Impact:**
<!-- Describe how this bug affects your use case -->

## Environment Information
**OpenSearch Version:**
<!-- e.g., OpenSearch 2.19 -->

**Additional Details:**
<!-- Any other relevant environment information -->
<!-- Exclude sensitive infrastructure details -->

## Screenshots
<!-- If applicable, add screenshots to help explain your problem -->
<!-- ⚠️ IMPORTANT: Ensure screenshots don't contain sensitive information -->
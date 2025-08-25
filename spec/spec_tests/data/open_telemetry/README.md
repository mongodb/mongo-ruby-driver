# OpenTelemetry Tests

______________________________________________________________________

## Testing

### Automated Tests

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
the OpenTelemetry specification. These tests utilize the
[Unified Test Format](../../unified-test-format/unified-test-format.md).

For each test, create a MongoClient, configure it to enable tracing.

```yaml
createEntities:
  - client:
      id: client0
      observeTracingMessages:
        enableCommandPayload: true
```

These tests require the ability to collect tracing [spans](../open-telemetry.md#span) data in a structured form as
described in the
[Unified Test Format specification.expectTracingMessages](../../unified-test-format/unified-test-format.md#expectTracingMessages).
For example the Java driver uses [Micrometer](https://jira.mongodb.org/browse/JAVA-5732) to collect tracing spans.

```yaml
expectTracingMessages:
  client: client0
  ignoreExtraSpans: false
  spans:
   ...
```

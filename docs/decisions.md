# Architecture Decision Log (ADR-lite)

## Prefix filtering in code (vs S3 notification prefix filter)
S3 prefix filters were inconsistent in testing; filtering by object key prefix in Lambda is simpler, testable, and less brittle.

## Idempotency approach
Used DynamoDB conditional writes on (tenant_id, event_id) to prevent duplicates under retries / at-least-once delivery.

## Bad JSON handling
Quarantine invalid JSON objects and record status in DynamoDB for traceability and replay.

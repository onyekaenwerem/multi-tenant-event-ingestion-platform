import json
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
DDB_TABLE = os.environ["DDB_TABLE"]

table = ddb.Table(DDB_TABLE)

def lambda_handler(event, context):
    # S3 event contains records; handle 1+ safely
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Only process tenant-scoped uploads (acts like the S3 prefix filter)
        if not key.startswith("tenant_id="):
            print(f"Skipping non-tenant object key={key}")
            continue

        # Only process objects from the RAW bucket (safety guard)
        if bucket != RAW_BUCKET:
            print(f"Skipping bucket={bucket}, expected RAW_BUCKET={RAW_BUCKET}")
            continue

        # Fetch object
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in key={key}: {e}")

            processed_at = datetime.now(timezone.utc).isoformat()
            fail_event_id = str(uuid.uuid4())

            # Move bad file to quarantine/ prefix in RAW bucket (copy + delete)
            quarantine_key = f"quarantine/dt={processed_at[:10]}/{fail_event_id}.json"

            s3.copy_object(
                Bucket=RAW_BUCKET,
                CopySource={"Bucket": bucket, "Key": key},
                Key=quarantine_key
            )
            s3.delete_object(Bucket=bucket, Key=key)

            # Record failure in DynamoDB (still tenant-aware if we can infer tenant from key)
            tenant_id = "unknown"
            if key.startswith("tenant_id="):
                # crude parse: tenant_id=<id>/...
                tenant_id = key.split("/")[0].split("=", 1)[1] or "unknown"
            
            table.put_item(
                Item={
                    "tenant_id": tenant_id,
                    "event_id": fail_event_id,
                    "event_type": "invalid_json",
                    "raw_bucket": bucket,
                    "raw_key": key,
                    "quarantine key": quarantine_key,
                    "processed_at": processed_at,
                    "status": "failed_json_parse"
                }
            )
    
            continue

        tenant_id = payload.get("tenant_id", "unknown")
        event_type = payload.get("event_type", "unknown")
        event_id = payload.get("event_id") or str(uuid.uuid4())
        processed_at = datetime.now(timezone.utc).isoformat()

        # Write metadata to DynamoDB
        table.put_item(
            Item={
                "tenant_id": tenant_id,
                "event_id": event_id,
                "event_type": event_type,
                "raw_bucket": bucket,
                "raw_key": key,
                "processed_at": processed_at,
                "status": "processed"
            }
        )

# Write processed/enriched output to PROCESSED bucket (same payload + metadata)
        processed_key = f"tenant_id={tenant_id}/dt={processed_at[:10]}/processed/{event_id}.json"
        out = {
            **payload,
            "event_id": event_id,
            "processed_at": processed_at,
            "source_raw_key": key
        }

        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=processed_key,
            Body=json.dumps(out).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"Processed tenant_id={tenant_id} event_id={event_id} raw_key={key} -> processed_key={processed_key}")

    return {"ok": True}

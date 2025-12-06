import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

DESIRED_TAG = os.getenv("DESIRED_TAG", "Environment")
ALL_REGIONS = ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "sa-east-1"]
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "UntaggedResources")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", 180))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 25))

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)
batch_buffer = []


def flush_batch():
    if not batch_buffer:
        return

    request_items = {
        DDB_TABLE_NAME: [{"PutRequest": {"Item": item}} for item in batch_buffer]
    }

    try:
        response = dynamodb.batch_write_item(RequestItems=request_items)
        unprocessed = response.get("UnprocessedItems", {}).get(DDB_TABLE_NAME, [])
        if unprocessed:
            print(
                f"Warning: {len(unprocessed)} items unprocessed"
            )
    except Exception as e:
        print(f"Failed to batch write {len(batch_buffer)} items: {e}")

    batch_buffer.clear()


def write_to_dynamodb(resource_arn, region):
    # Safely extract service (almost always present)
    service = resource_arn.split(":")[2] if ":" in resource_arn else "unknown"

    now = int(datetime.now(timezone.utc).timestamp())

    item = {
        "ARN": resource_arn,
        "AccountId": ACCOUNT_ID,
        "Region": region,
        "Service": service,
        "LastSeen": now,
        "ExpireAt": now + TTL_SECONDS,
    }

    batch_buffer.append(item)
    if len(batch_buffer) >= BATCH_SIZE:
        flush_batch()


def get_resources_without_tag_in_region(tag_key, region):
    client = boto3.client("resourcegroupstaggingapi", region_name=region)
    paginator = client.get_paginator("get_resources")
    count = 0

    try:
        for page in paginator.paginate(PaginationConfig={"PageSize": 100}):
            for mapping in page.get("ResourceTagMappingList", []):
                resource_arn = mapping["ResourceARN"]
                tags = {t["Key"]: t["Value"] for t in mapping.get("Tags", [])}

                if tag_key not in tags:
                    count += 1
                    print(f"{resource_arn=} {region=}")
                    write_to_dynamodb(resource_arn, region)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDeniedException":
            print(f"Warning: Access denied in {region} (skipping)")
        else:
            print(f"Error in {region}: {e}")

    return count


def main():
    total_untagged = 0
    for region in ALL_REGIONS:
        print(f"Scanning region: {region}")
        count = get_resources_without_tag_in_region(DESIRED_TAG, region)
        total_untagged += count
        print(f"Found {count} untagged resources in {region}")

    flush_batch()

    print(f"\nScan complete. Total untagged resources: {total_untagged}")
    print(
        f"All discovered untagged resources written to DynamoDB table '{DDB_TABLE_NAME}'"
    )


if __name__ == "__main__":
    main()

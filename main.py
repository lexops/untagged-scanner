import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

DESIRED_TAG = os.getenv("DESIRED_TAG", "Foobar")
ALL_REGIONS = [
    "Global",
    "us-east-1",
    "us-east-2",
]

DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "UntaggedResources")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", 86_400 * 1))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 25))

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)
batch_buffer = []

re_client = boto3.client("resource-explorer-2", region_name="us-east-1")


def build_item(resource: dict, ttl: int = TTL_SECONDS):
    now = int(datetime.now(timezone.utc).timestamp())
    return {
        "ARN": resource.get("Arn"),
        "AccountId": resource.get("OwningAccountId"),
        "Region": resource.get("Region"),
        "Service": resource.get("Service"),
        "ResourceType": resource.get("ResourceType"),
        "LastSeen": now,
        "ExpireAt": now + ttl,
    }


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
            print(f"Warning: {len(unprocessed)} items unprocessed")
    except Exception as e:
        print(f"Failed to batch write {len(batch_buffer)} items: {e}")

    batch_buffer.clear()


def write_to_dynamodb(item):
    batch_buffer.append(item)
    if len(batch_buffer) >= BATCH_SIZE:
        flush_batch()


def get_resources_without_tag_in_region(tag_key, region):
    query = f"resourcetype.supports:tags -tag.key:{tag_key} region:{region}"

    count = 0
    try:
        paginator = re_client.get_paginator("search")
        for page in paginator.paginate(
            QueryString=query, PaginationConfig={"PageSize": 100}
        ):
            for resource in page.get("Resources", []):
                item = build_item(resource)
                write_to_dynamodb(item)
                count += 1

    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDeniedException":
            print(f"Access denied in {region} (skipping)")
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


if __name__ == "__main__":
    main()

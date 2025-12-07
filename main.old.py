import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

DESIRED_TAG = os.getenv("DESIRED_TAG", "Environment")
ALL_REGIONS = [
    "us-east-1",
    "us-east-2",
]
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "UntaggedResources")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", 180))

sts_client = boto3.client("sts")
ACCOUNT_ID = sts_client.get_caller_identity()["Account"]
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)


def build_item(resource_arn: str, region: str):
    service = resource_arn.split(":")[2] if ":" in resource_arn else "unknown"
    now = int(datetime.now(timezone.utc).timestamp())

    return {
        "ARN": resource_arn,
        "AccountId": ACCOUNT_ID,
        "Region": region,
        "Service": service,
        "LastSeen": now,
        "ExpireAt": now + TTL_SECONDS,
    }


def get_resources_without_tag_in_region(tag_key: str, region: str):
    client = boto3.client("resourcegroupstaggingapi", region_name=region)
    paginator = client.get_paginator("get_resources")
    count = 0

    try:
        with table.batch_writer(overwrite_by_pname=["ARN"]) as batch:
            for page in paginator.paginate(PaginationConfig={"PageSize": 100}):
                for mapping in page.get("ResourceTagMappingList", []):
                    resource_arn = mapping["ResourceARN"]
                    tags = {t["Key"]: t["Value"] for t in mapping.get("Tags", [])}

                    if tag_key not in tags:
                        count += 1
                        print(f"{resource_arn=} {region=}")
                        item = build_item(resource_arn, region)
                        batch.put_item(Item=item)

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AccessDeniedException":
            print(f"Access denied in {region} (skipping)")
        elif error_code == "ThrottlingException":
            print(f"Throttled in {region} – consider adding sleep/backoff")
        else:
            print(f"Error in {region}: {e}")
    except Exception as e:
        print(f"Unexpected error in {region}: {e}")

    return count


def main():
    total_untagged = 0

    print(f"Starting scan for resources missing tag: {DESIRED_TAG}")
    print(f"Target table: {DDB_TABLE_NAME} | TTL: {TTL_SECONDS}s")

    for region in ALL_REGIONS:
        print(f"\nScanning region: {region}")
        count = get_resources_without_tag_in_region(DESIRED_TAG, region)
        total_untagged += count
        print(f"→ Found {count} untagged resources in {region}")

    print("\nScan complete!")
    print(f"Total untagged resources discovered: {total_untagged}")
    print(f"All items written to DynamoDB table '{DDB_TABLE_NAME}' with auto-expiry.")


if __name__ == "__main__":
    main()

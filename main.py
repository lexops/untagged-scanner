import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

DESIRED_TAG = os.getenv("DESIRED_TAG", "Foobar")
ALL_REGIONS = ["Global", "us-east-1"]
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "UntaggedResources")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", 86_400 * 1))

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)
re_client = boto3.client("resource-explorer-2", region_name="us-east-1")


def build_item(resource: dict):
    now = int(datetime.now(timezone.utc).timestamp())
    return {
        "ARN": resource["Arn"],
        "AccountId": resource.get("OwningAccountId"),
        "Region": resource.get("Region"),
        "Service": resource.get("Service"),
        "ResourceType": resource.get("ResourceType"),
        "LastSeen": now,
        "ExpireAt": now + TTL_SECONDS,
    }


def get_resources_without_tag_in_region(tag_key, region):
    query = f"resourcetype.supports:tags -tag.key:{tag_key} region:{region}"
    count = 0
    try:
        paginator = re_client.get_paginator("search")
        with table.batch_writer(overwrite_by_pkeys=["ARN","AccountId"]) as batch:
            for page in paginator.paginate(
                QueryString=query, PaginationConfig={"PageSize": 100}
            ):
                for resource in page.get("Resources", []):
                    item = build_item(resource)
                    batch.put_item(Item=item)
                    count += 1
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDeniedException":
            print(f"Access denied in {region} (skipping)")
        else:
            print(f"Error searching in {region}: {e}")
    except Exception as e:
        print(f"Unexpected error in {region}: {e}")
    return count


def main():
    total_untagged = 0
    for region in ALL_REGIONS:
        print(f"Scanning region: {region}")
        count = get_resources_without_tag_in_region(DESIRED_TAG, region)
        total_untagged += count
        print(f"Found {count} untagged resources in {region}")

    print(f"\nScan complete. Total untagged resources found: {total_untagged}")


if __name__ == "__main__":
    main()

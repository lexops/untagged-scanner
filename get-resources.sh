#!/usr/bin/env bash

REGION=${1:-'us-east-1'}
MAX_RESULTS=1000
# Ref: https://aws.amazon.com/pt/about-aws/whats-new/2024/05/aws-resource-explorer-provides-filtering-resources-support-tags/
SUPPORTS_TAGS="resourcetype.supports:tags"

QUERIES=(
    'tag:none' # Returns all resources without user-defined tags.
    # '-tag:*' # Returns all resources without tags
    # '-tag.key:*' # Same as '-tag:*'
)

for query in ${QUERIES[@]}; do
    filename="results-${REGION}-$(date +"%s").txt"
    
    aws resource-explorer-2 search \
        --region ${REGION} \
        --query-string "${REGION} ${SUPPORTS_TAGS} ${query}" \
        --max-results ${MAX_RESULTS} \
        --output json | jq -r '.Resources[].Arn' > ${filename}
    
    count=$(cat ${filename} | wc -l)
    echo "Query: ${query} found ${count} items."
done

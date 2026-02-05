# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Log request context for security audit
    request_context = event.get("requestContext", {})
    identity = request_context.get("identity", {})
    logger.info(
        f"Request received - RequestId: {context.request_id}, "
        f"SourceIP: {identity.get('sourceIp')}, "
        f"UserAgent: {identity.get('userAgent')}"
    )
    
    logger.info(f"Using DynamoDB table: {table}")
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(f"Processing payload with id: {item.get('id')}")
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            logger.info(f"Successfully inserted item with id: {id} into table: {table}")
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info("Processing request without payload - using default data")
            default_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
            logger.info(f"Successfully inserted default item with id: {default_id} into table: {table}")
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(
            f"Error processing request - RequestId: {context.request_id}, "
            f"Error: {str(e)}",
            exc_info=True
        )
        raise

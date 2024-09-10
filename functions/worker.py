import boto3
import botocore 
import json
import logging
import os 
import sys
import traceback

from utils import (
    get_local_sensitivities_df, 
    write_encrypted_csv_to_s3
)

s3 = boto3.client(
    "s3", 
    region_name="us-east-1", 
    config=botocore.config.Config(s3={"addressing_style":"path"})
)

s3_bucket = os.environ["S3_BUCKET_NAME"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_worker_output_to_s3(output_df, sqs_body):
    """ 
    Write csv output result to S3. 
    """
    job_id = sqs_body["job_id"]
    task_id = sqs_body["task_id"]
    s3_path = f"s3://{s3_bucket}/intermediate/{job_id}/{task_id}.csv"
    write_encrypted_csv_to_s3(output_df, s3_path)


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")
    try:
        # Parse SQS task
        sqs_body = json.loads(event["Records"][0]["body"])
        subset_s3_uri = sqs_body["subset_s3_uri"]
        script_s3_uri = sqs_body["script_s3_uri"]
        takeout_start_index = sqs_body["takeout_start_index"]
        takeout_end_index = sqs_body["takeout_end_index"]

        # Compute local sensitivity 
        output_df = get_local_sensitivities_df(script_s3_uri, subset_s3_uri, takeout_start_index, takeout_end_index)
        write_worker_output_to_s3(output_df, sqs_body) 

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exc_type, exc_value, exc_traceback)
        err_msg = json.dumps({
            "errorType": exc_type.__name__,
            "errorMessage": str(exc_value),
            "stackTrace": traceback_string 
        })
        logger.error(err_msg)
        
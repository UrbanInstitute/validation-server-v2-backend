import boto3
import botocore 
import datetime
import logging
import os 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client(
    "s3", 
    region_name="us-east-1", 
    config=botocore.config.Config(s3={"addressing_style":"path"})
)

s3_bucket = os.environ["S3_BUCKET_NAME"]


def compute_num_completed_tasks(job_id): 
    """
    Compute number of files in the job's worker output S3 directory. 

    Note: need to use paginator to get around 1000 item limit 
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=f"intermediate/{job_id}/")
    num_completed = 0 
    for page in pages: 
        num_completed += page["KeyCount"]
    return num_completed


def compute_num_remaining_tasks(event): 
    """
    Compute number of dispatched tasks that have not written output to S3.  
    """
    job_id = event["job_id"]
    num_dispatched = event["num_tasks_dispatched"]
    num_completed = compute_num_completed_tasks(job_id)
    num_remaining = num_dispatched - num_completed
    return num_remaining 


def compute_elapsed_secs(start_ftime): 
    """
    Compute number of seconds since state machine execution started. 
    """
    start_time = datetime.datetime.strptime(start_ftime, '%Y-%m-%dT%H-%M-%S')
    elapsed_secs = datetime.datetime.now() - start_time
    elapsed_secs = elapsed_secs.total_seconds()
    return elapsed_secs


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")
    num_remaining = compute_num_remaining_tasks(event)
    elapsed_secs = compute_elapsed_secs(event["start_time"])
    output = {
        **event, 
        "num_tasks_remaining": num_remaining, 
        "elapsed_secs": elapsed_secs
    }

    # Job finished (all tasks completed)
    if num_remaining == 0: 
        return {
            **output, 
            "completed": True
        }

    # Job timed out 
    class JobTimedOutException(Exception): pass
    if elapsed_secs > event["job_timeout_secs"]:  
        raise JobTimedOutException("Job timed out")

    # Job still running 
    return {
        **output, 
        "completed": False
    }

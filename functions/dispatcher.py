import boto3
import botocore 
import datetime
import json
import logging
import math
import numpy as np
import os 
import pandas as pd
import time

import config 

from utils import (
    get_dataset_metadata,
    get_local_sensitivities_df,  
    write_encrypted_csv_to_s3
)

s3 = boto3.client(
    "s3", 
    region_name="us-east-1", 
    config=botocore.config.Config(s3={"addressing_style":"path"})
)
sqs = boto3.client("sqs")

s3_bucket = os.environ["S3_BUCKET_NAME"]
sqs_queue = os.environ["TASK_QUEUE_NAME"]
sqs_queue_url = f"https://sqs.us-east-1.amazonaws.com/672001523455/{sqs_queue}"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_confidential_data(event): 
    """
    Read confidential data from S3. 
    """
    metadata = get_dataset_metadata(event["dataset_id"])
    dataset_s3_uri = metadata["dataset_s3_uri"]
    df = pd.read_csv(dataset_s3_uri)
    return df 


def write_subset_to_s3(subset, job_id, dataset_id, subset_index):
    """ 
    Write csv dataset subset to S3. 
    """
    s3_path = f"s3://{s3_bucket}/subsets/{job_id}/{dataset_id}_{subset_index}.csv"
    write_encrypted_csv_to_s3(subset, s3_path)
    return s3_path


def generate_task_id(dataset_id, subset_index, takeout_start_index, takeout_end_index):
    """
    Create unique task ID for a worker. 
    """
    id = f"{dataset_id}_{subset_index}_{takeout_start_index}_{takeout_end_index}"
    return id


def compute_takeout_end_index(takeout_start_index, max_index, workers_per_k):
    """
    Compute last takeout row for a worker. 
    """
    takeout_rows_per_worker = math.floor(max_index / workers_per_k) 
    takeout_end_index = min(takeout_start_index + takeout_rows_per_worker, max_index) 
    return takeout_end_index


def compute_workers_per_k(df, k, job_id, dataset_id, script_s3_uri): 
    """
    Compute minimum number of workers to assign to each subset to avoid hitting 
    the 900 seconds Lambda timeout (using 720 seconds for a buffer) based on the 
    time it takes to process 20 rows. 
    """
    # Define test parameters (semi-arbitrary)
    max_secs_per_worker = 720 # 900 sec (Lambda limit) - 180 sec (buffer)
    takeout_rows_to_test = 20 # Decide how many rows to test 

    # Create a sample subset with the same number of rows as a real subset 
    test_df = df.sample(frac = 1/k)
    rows_per_k = test_df.shape[0]
    test_s3_path = write_subset_to_s3(test_df, job_id, dataset_id, "_")
    
    # Time how long it takes to process 20 rows 
    t0 = time.time()
    get_local_sensitivities_df(script_s3_uri, test_s3_path, 1, takeout_rows_to_test)
    t1 = time.time()
    elapsed_secs = t1 - t0 

    # Compute maximum number of rows a worker can process 
    max_rows_per_worker = max_secs_per_worker * takeout_rows_to_test / elapsed_secs 

    # Compute minimum number of workers required to process each subset 
    min_workers_per_k = math.ceil(rows_per_k / max_rows_per_worker)
    return min_workers_per_k


def dispatch_task(job_id, dataset_id, subset_index, subset_s3_path, script_s3_uri, takeout_start_index, takeout_end_index):
    """
    Dispatch a single worker task as an SQS message. 
    """
    task_id = generate_task_id(dataset_id, subset_index, takeout_start_index, takeout_end_index)
    message = {
        "job_id": job_id,
        "task_id": task_id,
        "subset_s3_uri": subset_s3_path,
        "script_s3_uri": script_s3_uri,
        "takeout_start_index": takeout_start_index,
        "takeout_end_index": takeout_end_index,
    }
    response = sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=json.dumps(message))
    return response 


def dispatch_all_tasks(event):
    """
    Dispatch all worker tasks by randomly sampling from the full confidential 
    dataset, sharding the sample into k subsets (drawing without replacement), 
    splitting the subsets into smaller tasks by specifying takeout rows, and  
    dispatching messages to SQS with each task. 
    """
    # Parse submission info
    dataset_id = event["dataset_id"]
    job_id = event["job_id"]
    script_s3_uri = event["script_path"]
    sample_frac = config.SAMPLE_FRAC
    k = config.K 

    # Sample from full dataset
    # Note: Setting sample_frac = 1.0 randomly shuffles the full dataset
    df = load_confidential_data(event)
    sampled_df = df.sample(frac=sample_frac)

    # Compute number of workers to assign to each subset  
    workers_per_k = compute_workers_per_k(sampled_df, k, job_id, dataset_id, script_s3_uri)

    # Split into subsets
    df_subsets = np.array_split(sampled_df, k)

    # Write subsets to S3 and dispatch SQS tasks
    num_tasks = 0 
    for subset_index, subset in enumerate(df_subsets):
        subset_s3_path = write_subset_to_s3(subset, job_id, dataset_id, subset_index)

        # Continue dispatching tasks until all rows in the subset have been assigned
        max_index = subset.shape[0] 
        takeout_start_index = takeout_end_index = 1 # R starts indexing at 1 (not 0)!  
        while takeout_end_index < max_index: 
            takeout_end_index = compute_takeout_end_index(takeout_start_index, max_index, workers_per_k)
            dispatch_task(
                job_id,
                dataset_id,
                subset_index,
                subset_s3_path,
                script_s3_uri,
                takeout_start_index,
                takeout_end_index,
            )
            takeout_start_index = takeout_end_index + 1
            num_tasks += 1 
    
    return num_tasks


def update_state_machine(event, num_tasks): 
    """
    Update state machine payload with job monitoring parameters. 
    """
    start_time = datetime.datetime.now()
    start_ftime = start_time.strftime('%Y-%m-%dT%H-%M-%S')
    job_timeout_secs = int(os.environ["JOB_TIMEOUT_SECS"])
    
    return {
        **event, 
        "start_time": start_ftime, 
        "job_timeout_secs": job_timeout_secs, 
        "num_tasks_dispatched": num_tasks       
    }


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")
    num_tasks = dispatch_all_tasks(event)
    payload = update_state_machine(event, num_tasks)
    return payload 

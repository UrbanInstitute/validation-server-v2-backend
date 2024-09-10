import boto3
import botocore 
import logging
import os 
import pandas as pd

from utils import (
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


def get_worker_results(job_id): 
    """
    Get all worker results from S3 with a given job_id value. 

    Note: need to use paginator to get around 1000 item limit 
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=s3_bucket, Prefix=f"intermediate/{job_id}/"
    )
    df_list = [read_csv_from_s3(obj) for page in pages for obj in page["Contents"]] 
    results_df = pd.concat(df_list)
    return results_df 


def read_csv_from_s3(file):
    """
    Read a single csv file from S3. 
    """
    obj = s3.get_object(Bucket=s3_bucket, Key=file["Key"])
    obj_df = pd.read_csv(obj["Body"])
    return obj_df


def compute_mos_values(job_id): 
    """
    Compute maximum observed sensitivity (MOS) for all statistics. 

    MOS scaling parameter (chi) = max(n * ls), where 
        n = number of observations used to compute predicted value on the subset 
        ls = local sensitivity 
    """
    # Compute MOS values 
    results_df = get_worker_results(job_id)
    results_df["chi"] = results_df["n"] * results_df["ls"] 
    group_cols = [c for c in results_df.columns if c not in ("n", "ls", "chi")]
    mos_df = results_df.sort_values("chi", ascending=False).drop_duplicates(group_cols)
    mos_df.drop(columns = ["n", "ls"], inplace=True)
    return mos_df 


def get_true_values(job_id):
    """
    Read csv with true values (without noise added) from S3 (created by dispatcher). 
    """
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/true_output.csv"
    true_values_df = pd.read_csv(s3_path)
    return true_values_df


def prep_combined_output(job_id): 
    """ 
    Generate MOS formula inputs that are constant across runs.    
    """
    # Merge MOS values with true values 
    mos_df = compute_mos_values(job_id)
    true_values_df = get_true_values(job_id)
    merge_cols = [c for c in true_values_df.columns if c not in ("n", "value")] 
    combined_df = pd.merge(true_values_df, mos_df, how="left", on=merge_cols)

    # Add ID column for each statistic 
    statistic_id_col = combined_df.reset_index().index
    combined_df.insert(0, "statistic_id", statistic_id_col)

    # Add ID column for each analysis 
    analysis_id_col = combined_df.groupby(['analysis_name', 'analysis_type']).ngroup()
    combined_df.insert(1, "analysis_id", analysis_id_col)
    return combined_df 


def write_combined_output_to_s3(output_df, job_id):
    """ 
    Write MOS output csv file to S3. 
    """
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/mos_output.csv"
    write_encrypted_csv_to_s3(output_df, s3_path)
    return s3_path 


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")
    job_id = event["job_id"]
    combined_df = prep_combined_output(job_id)
    write_combined_output_to_s3(combined_df, job_id)
    return {
        **event, 
        "use_default_epsilon": True
    }
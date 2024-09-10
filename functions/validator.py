import boto3
import botocore
import logging
import os 
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects.conversion import localconverter

from utils import (
    get_dataset_metadata,
    get_rpy_conversion_rules, 
    load_user_script, 
    send_email_to_user, 
    update_job_status,
    write_encrypted_csv_to_s3, 
)

s3 = boto3.client(
    "s3", 
    region_name="us-east-1", 
    config=botocore.config.Config(s3={"addressing_style":"path"})
)

s3_bucket = os.environ["S3_BUCKET_NAME"]

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


def get_output_df(script_s3_uri, df_s3_uri):  
    """
    Use rpy2 to read a csv from S3, run the analysis (R script must contain the 
    run_analysis() function) and return the output as a pandas df. 
    """
    ro.r(
    """
    compute_output <- function(data_s3_uri) {
        df <- aws.s3::s3read_using(read.csv, object = data_s3_uri)
        output <- run_analysis(df)
        return(output)
    }
    """)
    load_user_script(script_s3_uri)
    rpy2_conversion_rules = get_rpy_conversion_rules()
    with localconverter(rpy2_conversion_rules): 
        output_df_r = ro.r["compute_output"](df_s3_uri)
        output_df_pd = ro.conversion.rpy2py(output_df_r)
    return output_df_pd


def compute_true_values(event):
    """
    Run R script against the full dataset.  
    """
    script_s3_uri = event["script_path"]
    metadata = get_dataset_metadata(event["dataset_id"])
    dataset_s3_uri = metadata["dataset_s3_uri"]
    output_df = get_output_df(script_s3_uri, dataset_s3_uri)
    return output_df


def write_true_output_to_s3(output_df, event):
    """ 
    Write csv with true values (results from full dataset without noise added) to S3. 
    """
    job_id = event["job_id"]
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/true_output.csv"
    write_encrypted_csv_to_s3(output_df, s3_path)
    return s3_path 


def send_success_job_submission_email(event): 
    """ 
    Send user an email indicating job was successfully submitted. 
    """
    subject = "Validation Server Job Status" 
    body = "Job was successfully submitted."
    send_email_to_user(event, subject, body)


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")
    true_output_df = compute_true_values(event) 
    write_true_output_to_s3(true_output_df, event) 
    result = {
        "ok": True, 
        "info": "running"
    }   
    update_job_status(event, result)
    send_success_job_submission_email(event)
    return event

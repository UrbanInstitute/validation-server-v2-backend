import boto3
import botocore 
import logging
import math
import numpy as np 
from numpy.random import default_rng
import os 
import pandas as pd

import config 

from utils import (
    send_email_to_user,
    update_job_status,  
    update_run_status, 
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


def add_noise_to_value(row): 
    """
    Add noise to a single estimate based on the MOS formula. 
    """
    if row["statistic"] in ["n", "nobs"]: 
        noise = math.sqrt(2) * row["omega"] / row["epsilon"]
    else: 
        noise = math.sqrt(2) * row["chi"] / (row["epsilon"] * row["n"]) * row["omega"]
    
    value_sanitized = row["value"] + noise
    return value_sanitized


def get_unchanged_sanitized_values(event): 
    """ 
    Get sanitized values from the previous run for all statistics where the user 
    did NOT update epsilon values.  
    """
    job_id = event["job_id"]
    run_id = event["run_id"]
    previous_run_id = run_id - 1

    # Get sanitized results from previous run 
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/sanitized_output_{previous_run_id}.csv"
    df = pd.read_csv(s3_path)

    # Drop rows with user-updated epsilon values in the current run 
    new_ids = [e["statistic_id"] for e in event["epsilons"]]
    df.drop(df[df["statistic_id"].isin(new_ids)].index, inplace=True)
    return df 


def compute_sanitized_values(event, df):
    """
    Apply MOS formulas to generate output to return to the researcher. 
    """
    job_id = event["job_id"]
    run_id = event["run_id"]
    use_default_epsilon = event["use_default_epsilon"]

    rng = default_rng() 
    df["omega"] = rng.standard_normal(df.shape[0])
    df["noise_90"] = add_noise_pct_col(df, pct=90, n_samples=100)
    df["value_sanitized"] = df.apply(add_noise_to_value, axis=1)
    df.drop(columns = ["value", "n", "omega"], inplace=True)

    # Append rows from previous run for rows where user didn't update epsilon 
    if not use_default_epsilon: 
        unchanged_df = get_unchanged_sanitized_values(event)
        df = pd.concat([df, unchanged_df])
        df.sort_values(by="statistic_id", inplace=True)
    return df


def get_mos_values(job_id): 
    """
    Read csv with MOS values from S3 (created by combiner). 
    """
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/mos_output.csv"
    df = pd.read_csv(s3_path)
    return df


def add_noise_pct_col(df, pct, n_samples): 
    """
    Compute percentile estimate of noise to display to the user based on 
    chi, n, and omega (pct percentile of absolute value of n_samples drawn from 
    standard normal distribution). 

    Used for generating a graph showing the epsilon-noise tradeoff. 
    """
    rng = default_rng() 
    omega_noise_pct = np.percentile(abs(rng.standard_normal(n_samples)), pct)
    noise_pct = math.sqrt(2) * (df["chi"]/df["n"]) * omega_noise_pct
    return noise_pct 


def add_default_epsilon_col(df):
    """
    Add column to df with parameter-level epsilon values based on the default 
    analysis-level epsilon value. 
    """
    # Mask small cell counts 
    df = df[df["n"] >= config.N_THRESHOLD]

    # Compute default epsilon value 
    df["epsilon"] = config.DEFAULT_EPSILON / df.shape[0] 
    return df


def prep_output(event):
    """
    Join epsilon values with MOS formula inputs. 
    """
    job_id = event["job_id"]
    use_default_epsilon = event["use_default_epsilon"]
    mos_df = get_mos_values(job_id)
    if use_default_epsilon: 
        prepped_df = add_default_epsilon_col(mos_df)
    else: # User-specified epsilon values
        epsilon_df = pd.DataFrame(event["epsilons"])
        prepped_df = pd.merge(epsilon_df, mos_df, how="left", on="statistic_id")
    
    return prepped_df


def write_sanitized_output_to_s3(event, output_df):
    """ 
    Write final sanitized output csv file to S3. 
    """
    job_id = event["job_id"]
    run_id = event["run_id"]
    s3_path = f"s3://{s3_bucket}/submissions/{job_id}/sanitized_output_{run_id}.csv"
    write_encrypted_csv_to_s3(output_df, s3_path)
    return s3_path


def send_results_email(event): 
    """ 
    Send user an email indicating results are available. 
    """
    subject = "Validation Server Results" 
    body = "Results are available."
    send_email_to_user(event, subject, body)


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")    
    prepped_df = prep_output(event)
    sanitized_df = compute_sanitized_values(event, prepped_df)
    write_sanitized_output_to_s3(event, sanitized_df)
    result = {
        "ok": True, 
        "info": "completed"    
    }   
    update_job_status(event, result)
    update_run_status(event, result)
    send_results_email(event)
    return event

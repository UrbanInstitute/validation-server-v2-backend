import base64
import boto3
import json 
import os 
import pandas as pd
import requests 
import s3fs

from botocore.exceptions import ClientError
import rpy2.robjects as ro
from rpy2 import rinterface as ri
from rpy2.rinterface_lib import na_values
from rpy2.robjects.conversion import localconverter, get_conversion

s3_bucket = os.environ["S3_BUCKET_NAME"]


def get_secret(secret_name = "sdt-validation-server-engine"):
    """
    Retrieve engine credentials from AWS Secrets Manager. 
    """
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return json.loads(secret)


def get_api_token(): 
    """
    Generate API token for engine. 
    """
    credentials = get_secret()
    user_account = {
        "email": credentials["engine_email"],  
        "password": credentials["engine_password"] 
    }

    url_stub = "https://sdt-validation-server.urban.org/api" 
    url = f"{url_stub}/users/login/" 
    r = requests.post(url, data=user_account)
    token = r.json()["token"]
    return token 


def update_job_status(event, result): 
    """
    PATCH job status to API. 
    """
    job_id = event["job_id"]
    url_stub = "https://sdt-validation-server.urban.org/api" 
    url = f"{url_stub}/job/jobs/{job_id}/" 

    token = get_api_token()
    headers = {"Authorization": f"Token {token}"}
    payload = {"status": json.dumps(result)}

    r = requests.patch(url, data=payload, headers=headers)
    return r 


def update_run_status(event, result): 
    """
    PATCH job status to API. 
    """
    job_id = event["job_id"]
    run_id = event["run_id"]
    url_stub = "https://sdt-validation-server.urban.org/api" 
    url = f"{url_stub}/job/jobs/{job_id}/runs/{run_id}/" 

    token = get_api_token()
    headers = {"Authorization": f"Token {token}"}
    payload = {"status": json.dumps(result)}

    r = requests.patch(url, data=payload, headers=headers)
    return r 


def get_rpy_conversion_rules(): 
    """
    Custom rpy2 conversion rules to better handle NAs from R to pandas dfs. 
    Borrowed from here: https://stackoverflow.com/a/72670945 
    """
    df_rules = ro.default_converter

    @df_rules.rpy2py.register(ri.IntSexpVector)
    def to_int(obj):
        return [int(v) if v != na_values.NA_Integer else pd.NA for v in obj]

    @df_rules.rpy2py.register(ri.FloatSexpVector)
    def to_float(obj):
        return [float(v) if v != na_values.NA_Real else pd.NA for v in obj]

    @df_rules.rpy2py.register(ri.StrSexpVector)
    def to_str(obj):
        return [str(v) if v != na_values.NA_Character else pd.NA for v in obj]

    @df_rules.rpy2py.register(ri.BoolSexpVector)
    def to_bool(obj):
        return [bool(v) if v != na_values.NA_Logical else pd.NA for v in obj]

    # Define the top-level converter
    def toDataFrame(obj):
        cv = get_conversion() # Get the converter from current context
        return pd.DataFrame(
            {str(k): cv.rpy2py(obj[i]) for i, k in enumerate(obj.names)}
        )

    # Associate the converter with R data.frame class
    df_rules.rpy2py_nc_map[ri.ListSexpVector].update({"data.frame": toDataFrame})
    return df_rules 


def load_user_script(script_s3_uri): 
    """
    Use rpy2 to source an arbitrary R script from S3. 
    """
    ro.r(
    """
    library(validationserver)
    load_script_from_s3 <- function(script_s3_uri) {
        aws.s3::s3source(script_s3_uri)
    }
    """)
    ro.r["load_script_from_s3"](script_s3_uri)


def get_local_sensitivities_df(script_s3_uri, subset_s3_uri, takeout_start_index, takeout_end_index):
    """
    Implement MOS algorithm to compute local sensitivities for subset (maximum difference 
    between predicted value on full subset and predicted value from removing one observation) 
    for each statistic.  

    The algorithm is implemented as a function in R and called using rpy2 to minimize 
    the conversion between R and Python. 

    Args:
        script_s3_uri (str): path to R script on S3 
        subset_s3_uri (str): path to csv subset on S3 
        takeout_start_index (int): first row index in subset to take out  
        takeout_end_index (int): last row index in subset to take out  

    Returns:
        pandas df with local sensitivities for each statistic   
    """
    ro.r(
    """ 
    compute_local_sensitivities <- function(data_s3_uri, takeout_start_index, takeout_end_index) {
        # Read subset from S3
        df <- aws.s3::s3read_using(read.csv, object = data_s3_uri)
        
        # Compute estimates on full subset
        output_full <- run_analysis(df)
        merge_cols <- names(output_full)[!(names(output_full) %in% c("value", "n"))]
        output_full <- rename(output_full, c("value_full" = "value", "n_full" = "n"))
        
        output_full$max_sensitivity <- 0 # Initialize at 0
        for (takeout_index in takeout_start_index:takeout_end_index) {
            if (takeout_index %% 500 == 0) {
                message(paste("Taking out row", takeout_index, 'out of', takeout_end_index))
            } 
            # Re-compute estimates removing one observation at a time
            df_takeout <- df[-takeout_index,]
            output_takeout <- run_analysis(df_takeout)
            
            # Update max sensitivity for each statistic
            output_full <- merge(output_full, output_takeout, by = merge_cols, all.x = TRUE)
            output_full$max_sensitivity <- pmax(abs(output_full$value_full - output_full$value), output_full$max_sensitivity)
            output_full <- output_full[,!(names(output_full) %in% c("value", "n"))]
        }
        
        # Format output columns 
        output_full <- output_full[,!(names(output_full) %in% "value_full")]
        output_full <- rename(output_full, c("n" = "n_full", "ls" = "max_sensitivity"))
        return(output_full)
    }
    """)
    load_user_script(script_s3_uri)
    rpy2_conversion_rules = get_rpy_conversion_rules()
    with localconverter(rpy2_conversion_rules): 
        output_df_r = ro.r["compute_local_sensitivities"](subset_s3_uri, takeout_start_index, takeout_end_index)
        output_df_pd = ro.conversion.rpy2py(output_df_r)
    return output_df_pd 


def get_dataset_metadata(dataset_id):
    """
    Get dictionary with metadata info from dataset name. 
    """
    dataset_metadata = [
        {
            "dataset_id": "cps",
            "dataset_s3_uri": f"s3://{s3_bucket}/data/cps_2022-2023.csv",
        },
        {
            "dataset_id": "puf_2012",
            "dataset_s3_uri": f"s3://{s3_bucket}/data/puf_2012.csv",
        }, 
        {
            "dataset_id": "puf_2012_subset",
            "dataset_s3_uri": f"s3://{s3_bucket}/data/puf_2012_subset.csv",
        }, 
    ]
    metadata = [d for d in dataset_metadata if d["dataset_id"] == dataset_id][0]
    return metadata
    

def write_encrypted_csv_to_s3(df, s3_path, index=False): 
    """
    Use a configured s3fs filesystem to specify KMS SSE when writing a pandas df 
    as a csv to an encrypted S3 bucket. 
    """
    fs = s3fs.S3FileSystem(
        s3_additional_kwargs = {
            "ServerSideEncryption": "aws:kms"
        }
    )
    with fs.open(s3_path, "w") as f:
        df.to_csv(f, index=index)


def send_email_to_user(event, subject, body):
    # Create an SES client
    client = boto3.client('ses', region_name='us-east-1')

    # Specify the email details
    sender = os.environ['SES_SENDER']
    recipient = event['user_email']

    # Send the email
    response = client.send_email(
        Source=sender,
        Destination={
            'ToAddresses': [
                recipient,
            ],
        },
        Message={
            'Subject': {
                'Data': subject,
            },
            'Body': {
                'Text': {
                    'Data': body,
                },
            },
        },
    )

    return response
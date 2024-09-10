import boto3
import datetime
import json

sfn_client = boto3.client("stepfunctions")

env = "-stg"

sf_arn = f"arn:aws:states:us-east-1:672001523455:stateMachine:sdt-validation-server-statemachine{env}"

job_id = 1
run_id = 1

params = {
  "job_id": job_id,
  "run_id": run_id, 
  "user_email": "etyagi@urban.org",
  "dataset_id": "cps",
  "script_path": f"s3://sdt-validation-server{env}/test-scripts/cps-multi.R",
}

# Use timestamp to ensure that step function names are unique while testing 
start_time = datetime.datetime.now()
start_ftime = start_time.strftime('%Y-%m-%dT%H-%M-%S')

response = sfn_client.start_execution(
    stateMachineArn = sf_arn,
    name = f"{job_id}_{run_id}_{start_ftime}",
    input = json.dumps(params)
)
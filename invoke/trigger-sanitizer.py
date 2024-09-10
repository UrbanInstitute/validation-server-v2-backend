import boto3
import json

client = boto3.client("lambda")

env = "-stg"

payload = {
  "job_id": 1,
  "run_id": 2, 
  "user_email": "etyagi@urban.org",
  "use_default_epsilon": False, 
  "epsilons":[
      {
         "statistic_id": 1,
         "epsilon": 1
      },
      {
         "statistic_id": 2,
         "epsilon": 2,
      }
   ]
}


payload = json.dumps(payload).encode()
response = client.invoke(FunctionName=f"sdt-validation-server-sanitizer{env}", InvocationType="Event", Payload=payload)
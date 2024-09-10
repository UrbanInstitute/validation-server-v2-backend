import logging

from utils import (
    send_email_to_user, 
    update_job_status
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_failure_email(event): 
    """ 
    Send user an email indicating job failed. 
    """
    subject = "Validation Server Job Status" 
    body = "There was an error processing your submission."
    send_email_to_user(event, subject, body)


def lambda_handler(event, context):
    logger.info(f"Input event: {event}")

    if event["error"]["Error"] == "RRuntimeError": 
        result = {
            "ok": False, 
            "info": "failed",
            "errormsg": """
                There was an error in your program. 
                For privacy reasons, we cannot provide additional information. 
                Please revise your program and resubmit.
                """
        }

    else: 
        result = {
            "ok": False, 
            "info": "failed",
            "errormsg": "Encountered unexpected error."
        }
        
    update_job_status(event, result)
    send_failure_email(event)


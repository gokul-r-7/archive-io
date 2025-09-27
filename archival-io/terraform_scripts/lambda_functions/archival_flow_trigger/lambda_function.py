import json
import boto3
import uuid
from datetime import datetime

sfn = boto3.client("stepfunctions")

# Replace with your Step Function ARN
STEP_FUNCTION_ARN = "arn:aws:states:us-east-1:894168368672:stateMachine:archival"

def lambda_handler(event, context):
    """
    API Gateway Proxy -> Lambda -> Step Function
    """

    # Parse request body (API GW passes as string sometimes)
    body = event.get("body")
    print("Event : ", json.dumps(event))
    if isinstance(body, str):
        body = json.loads(body)

    run_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()

    # Build glue job args for each table
    archive_args = []
    for tbl in body.get("source_table", []):
        job_args = {
            "--glue_connection": body["glue_connection"],
            "--legal_hold": str(body["legal_hold"]).lower(),
            "--retention_policy": str(body["retention_policy"]),
            "--source_database": body["source_database"],
            "--source_schema": body["source_schema"],
            "--source_table": tbl["table_name"],
            "--target_database": body["source_database"],
            "--target_table": f"{body['source_schema']}_{tbl['table_name']}",
            "--target_s3_path": f"s3://archival-io-227/iceberg_warehouse/archived_data/{body['source_schema']}_{tbl['table_name']}/"
        }
        if "condition" in tbl:
            job_args["--condition"] = tbl["condition"]
        if "query" in tbl:
            job_args["--query"] = tbl["query"]

        archive_args.append(job_args)

    # Step Function input
    sfn_input = {
        "run_id": run_id,
        "timestamp": timestamp,
        "triggered_by": body.get("triggered_by"),
        "triggered_by_email": body.get("triggered_by_email"),
        "archive_args": archive_args,
        "input_payload": body
    }

    # Start Step Function execution
    response = sfn.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=f"archival-{run_id}",
        input=json.dumps(sfn_input)
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Archival workflow triggered",
            "executionArn": response["executionArn"],
            "run_id": run_id
        })
    }

"""
Minimal S3-event trigger — replaces the routing Lambda.
Receives S3 PutObject event, starts Glue job with the file path as input.
All processing logic lives in glue_skp_pipeline.py.
"""
import json
import os
import boto3

glue = boto3.client('glue')

GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
PROCESSED_BUCKET = os.environ['PROCESSED_BUCKET']
APP_ENV = os.environ.get('APP_ENV', 'dev')


def handler(event, context):
    record = event['Records'][0]['s3']
    bucket = record['bucket']['name']
    key = record['object']['key']

    input_path = f's3://{bucket}/{key}'
    output_path = f's3://{PROCESSED_BUCKET}/processed/'

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            '--input_path': input_path,
            '--output_path': output_path,
            '--env': APP_ENV,
            '--config': '{}',
        },
    )

    run_id = response['JobRunId']
    print(f"Started Glue job {GLUE_JOB_NAME} | run={run_id} | input={input_path}")
    return {'statusCode': 200, 'jobRunId': run_id}

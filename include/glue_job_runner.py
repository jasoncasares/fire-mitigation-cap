from aws_secret_manager import get_secret
from glue_job_submission import create_glue_job
import os

s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-1"
aws_access_key_id = get_secret("AWS_ACCESS_KEY_ID")
aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")
kafka_credentials = get_secret("KAFKA_CREDENTIALS")

def create_and_run_glue_job(job_name, script_path, arguments):
    glue_job = create_glue_job(
                    job_name=job_name,
                    script_path=script_path,
                    arguments=arguments,
                    aws_region=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    s3_bucket=s3_bucket,
                    catalog_name=catalog_name,
                    kafka_credentials=kafka_credentials,
                    )



local_script_path = os.path.join("include", '/scripts/nasa_backfill.py')
create_and_run_glue_job('nasa_backfill',
                        script_path=local_script_path,
                        arguments={'--ds': '2024-06-18', '--output_table': 'jasoncasares.nasa_firms'})
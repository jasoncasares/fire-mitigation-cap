import os
import logging
from aws_secret_manager import test_aws_connectivity
from glue_job_submission import create_glue_job, check_job_status

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def find_script(script_name, start_path='.'):
    for root, dirs, files in os.walk(start_path):
        if script_name in files:
            return os.path.join(root, script_name)
    return None

def create_and_run_glue_job(job_name, script_path, arguments):
    # Get S3 bucket from environment variable
    s3_bucket = os.getenv('AWS_S3_BUCKET')

    if not s3_bucket:
        logging.error("AWS_S3_BUCKET environment variable is not set. Cannot create Glue job.")
        return

    try:
        glue_job = create_glue_job(
            job_name=job_name,
            script_path=script_path,
            arguments=arguments,
            s3_bucket=s3_bucket
        )
        logging.info(f"Glue job '{job_name}' created and run successfully.")
        return glue_job
    except Exception as e:
        logging.error(f"Failed to create or run Glue job '{job_name}': {str(e)}")
        return None

if __name__ == "__main__":
    # Test AWS connectivity
    if not test_aws_connectivity():
        logging.error("AWS connectivity test failed. Please check your AWS configuration.")
        exit(1)

    # Print current working directory
    logging.info(f"Current working directory: {os.getcwd()}")

    # Find the script
    script_name = 'nasa_backfill.py'
    script_path = find_script(script_name)

    if script_path:
        logging.info(f"Script found at: {script_path}")

        job = create_and_run_glue_job(
            'nasa_backfill',
            script_path=script_path,
            arguments={
                '--ds': '2024-06-18', 
                '--output_table': 'iceberg.fire_mitigation_db.nasa_firms',
                '--s3_bucket': os.getenv('AWS_S3_BUCKET')
            }
        )

        if job:
            logging.info("Job created and run successfully.")
        else:
            logging.error("Failed to create or run job.")
    else:
        logging.error(f"Script '{script_name}' not found in the project directory or its subdirectories.")
        # List contents of the current directory
        logging.info(f"Contents of current directory: {os.listdir('.')}")
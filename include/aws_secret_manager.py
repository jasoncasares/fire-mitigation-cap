import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_aws_connectivity():
    print("Testing AWS connectivity...")
    try:
        # Use default credentials from environment variables
        session = boto3.Session()
        sts = session.client('sts')
        response = sts.get_caller_identity()
        print(f"Successfully connected to AWS. Account ID: {response['Account']}")
        return True
    except NoCredentialsError:
        print("No AWS credentials found. Please check your environment variables or AWS configuration.")
    except ClientError as e:
        print(f"AWS ClientError: {e}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    return False

if __name__ == "__main__":
    test_aws_connectivity()
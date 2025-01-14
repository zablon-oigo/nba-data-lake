import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Define the names of resources to delete
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
GLUE_DATABASE_NAME = os.getenv("GLUE_DATABASE_NAME")

def bucket_exists(bucket_name):
    """Check if an S3 bucket exists."""
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False

def delete_athena_query_results(bucket_name):
    """Delete Athena query results stored in the specified S3 bucket."""
    s3 = boto3.client("s3")
    if not bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist. Skipping Athena query result deletion.")
        return
    try:
        print(f"Deleting Athena query results in bucket: {bucket_name}")
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="athena-results/")
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
                print(f"Deleted Athena query result: {obj['Key']}")
        else:
            print(f"No Athena query results found in bucket {bucket_name}.")
    except ClientError as e:
        print(f"Error deleting Athena query results in bucket {bucket_name}: {e}")

def delete_s3_bucket(bucket_name):
    """Delete a specific S3 bucket and its contents."""
    s3 = boto3.client("s3")
    if not bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist. Skipping deletion.")
        return
    try:
        print(f"Deleting bucket: {bucket_name}")
        # Delete all objects in the bucket
        objects = s3.list_objects_v2(Bucket=bucket_name)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
                print(f"Deleted object: {obj['Key']}")
        # Delete the bucket
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")
    except ClientError as e:
        print(f"Error deleting bucket {bucket_name}: {e}")

def delete_glue_resources(database_name):
    """Delete Glue database and associated tables."""
    glue = boto3.client("glue")
    try:
        print(f"Deleting Glue database: {database_name}")
        # Get tables in the database
        tables = glue.get_tables(DatabaseName=database_name)["TableList"]
        for table in tables:
            table_name = table["Name"]
            print(f"Deleting Glue table: {table_name} in database {database_name}")
            glue.delete_table(DatabaseName=database_name, Name=table_name)
        # Delete the database
        glue.delete_database(Name=database_name)
        print(f"Deleted Glue database: {database_name}")
    except ClientError as e:
        print(f"Error deleting Glue resources for database {database_name}: {e}")

def main():
    print("Deleting resources created during data lake setup...")
    delete_athena_query_results(BUCKET_NAME)
    delete_s3_bucket(BUCKET_NAME)
    delete_glue_resources(GLUE_DATABASE_NAME)
    print("All specified resources deleted successfully.")

if __name__ == "__main__":
    main()

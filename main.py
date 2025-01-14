import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

load_dotenv()

# AWS configurations
region = "us-east-1"  
bucket_name = os.getenv("AWS_BUCKET_NAME")
glue_database_name = os.getenv("GLUE_DATABASE_NAME")
athena_output_location = f"s3://{bucket_name}/athena-results/"
api_key = os.getenv("API_KEY")
nba_endpoint = os.getenv("NBA_ENDPOINT")

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)

def create_s3_bucket():
    """Create an S3 bucket for storing sports data."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")

def create_glue_database():
    """Create a Glue database for the data lake."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")

def fetch_nba_data():
    """Fetch NBA player data from sportsdata.io."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()
        print("Fetched NBA data successfully.")
        return response.json()
    except Exception as e:
        print(f"Error fetching NBA data: {e}")
        return []

def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload NBA data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/nba_player_data.jsonl"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

def create_glue_table():
    """Create a Glue table for the data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Experience", "Type": "int"},
                        {"Name": "Height", "Type": "int"},
                        {"Name": "Weight", "Type": "int"},
                        {"Name": "Salary", "Type": "int"}
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table 'nba_players' created successfully.") 
    except Exception as e:
        print(f"Error creating Glue table: {e}")

def configure_athena():
    """Set up Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        print("Athena output location configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

def run_athena_query(query, database_name, output_location):
    """Run a query in Athena."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database_name},
            ResultConfiguration={"OutputLocation": output_location},
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Athena query started. Execution ID: {query_execution_id}")
        return query_execution_id
    except Exception as e:
        print(f"Error running Athena query: {e}")
        return None

def wait_for_query_to_complete(query_execution_id):
    """Wait for an Athena query to complete and log errors if it fails."""
    try:
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if state == 'FAILED':
                    print("Query execution failed. Error details:")
                    print(response['QueryExecution']['Status']['StateChangeReason'])
                return state
            time.sleep(2)
    except Exception as e:
        print(f"Error while waiting for query completion: {e}")
        return "FAILED"

def get_query_results(query_execution_id):
    """Fetch and display query results from Athena."""
    try:
        response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = response['ResultSet']['Rows']
        print("Query Results:")
        for row in rows:
            print([col.get('VarCharValue', '') for col in row['Data']])
    except Exception as e:
        print(f"Error fetching query results: {e}")

def query_nba_data():
    """Run a SQL query to analyze NBA data in Athena."""
    query = f"""
    SELECT 
        Team, 
        COUNT(PlayerID) AS PlayerCount,
        SUM(Salary) AS TotalSalary
    FROM {glue_database_name}.nba_players
    GROUP BY Team
    LIMIT 20;
    """
    print("Running SQL query in Athena...")
    query_execution_id = run_athena_query(query, glue_database_name, athena_output_location)
    
    if query_execution_id:
        status = wait_for_query_to_complete(query_execution_id)
        if status == "SUCCEEDED":
            print("Query executed successfully. Fetching results...")
            get_query_results(query_execution_id)
        else:
            print("Query execution failed.")


# Main workflow
def main():
    print("Setting up data lake for NBA sports analytics...")
    create_s3_bucket()
    time.sleep(5) 
    create_glue_database()
    nba_data = fetch_nba_data()
    if nba_data: 
        upload_data_to_s3(nba_data)
    create_glue_table()
    configure_athena()
    query_nba_data()
    print("Data lake setup complete.")

if __name__ == "__main__":
    main()

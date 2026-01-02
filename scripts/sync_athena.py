#!/usr/bin/env python3
"""
AWS Athena Data Sync Script

This script:
1. Runs Athena queries defined in /sources/aws_athena/*.sql
2. Waits for query completion
3. Downloads results to local Parquet/CSV
4. Loads into DuckDB for Evidence to query

Usage:
    python scripts/sync_athena.py [--query query_name]
    
Environment Variables:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    ATHENA_WORKGROUP (optional, default: 'primary')
    ATHENA_OUTPUT_BUCKET (required, e.g., 's3://my-athena-results/')
    ATHENA_DATABASE (required)
"""

import os
import sys
import time
import glob
import boto3
import duckdb
from pathlib import Path

# Configuration
SOURCES_DIR = Path(__file__).parent.parent / 'sources' / 'aws_athena'
DATA_DIR = Path(__file__).parent.parent / 'data'
DUCKDB_PATH = DATA_DIR / 'athena_cache.duckdb'

def get_athena_client():
    return boto3.client(
        'athena',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION', 'us-east-1')
    )

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION', 'us-east-1')
    )

def run_athena_query(client, query: str, database: str, workgroup: str, output_location: str) -> str:
    """Execute Athena query and return execution ID."""
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        WorkGroup=workgroup,
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def wait_for_query(client, execution_id: str, poll_interval: int = 2) -> dict:
    """Wait for query to complete and return execution details."""
    while True:
        response = client.get_query_execution(QueryExecutionId=execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            return response['QueryExecution']
        
        time.sleep(poll_interval)

def download_results(s3_client, s3_uri: str, local_path: Path):
    """Download query results from S3."""
    # Parse s3://bucket/key format
    parts = s3_uri.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]
    
    s3_client.download_file(bucket, key, str(local_path))

def load_sql_queries() -> dict:
    """Load all .sql files from the Athena sources directory."""
    queries = {}
    for sql_file in SOURCES_DIR.glob('*.sql'):
        query_name = sql_file.stem
        with open(sql_file, 'r') as f:
            queries[query_name] = f.read()
    return queries

def sync_to_duckdb(csv_files: dict):
    """Load CSV results into DuckDB."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(str(DUCKDB_PATH))
    
    for table_name, csv_path in csv_files.items():
        print(f"Loading {table_name} from {csv_path}")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
        
        # Verify
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  Loaded {count} rows")
    
    con.close()

def main():
    # Parse arguments
    query_filter = None
    if len(sys.argv) > 2 and sys.argv[1] == '--query':
        query_filter = sys.argv[2]
    
    # Validate environment
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'ATHENA_OUTPUT_BUCKET', 'ATHENA_DATABASE']
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    database = os.environ['ATHENA_DATABASE']
    workgroup = os.environ.get('ATHENA_WORKGROUP', 'primary')
    output_location = os.environ['ATHENA_OUTPUT_BUCKET']
    
    # Initialize clients
    athena = get_athena_client()
    s3 = get_s3_client()
    
    # Load queries
    queries = load_sql_queries()
    if query_filter:
        queries = {k: v for k, v in queries.items() if k == query_filter}
    
    if not queries:
        print("No queries found to execute")
        sys.exit(0)
    
    # Execute queries
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_files = {}
    
    for name, sql in queries.items():
        print(f"\nExecuting query: {name}")
        
        try:
            execution_id = run_athena_query(athena, sql, database, workgroup, output_location)
            print(f"  Execution ID: {execution_id}")
            
            result = wait_for_query(athena, execution_id)
            state = result['Status']['State']
            
            if state == 'SUCCEEDED':
                # Download results
                output_uri = result['ResultConfiguration']['OutputLocation']
                local_csv = DATA_DIR / f"{name}.csv"
                download_results(s3, output_uri, local_csv)
                csv_files[name] = str(local_csv)
                print(f"  Downloaded to {local_csv}")
            else:
                reason = result['Status'].get('StateChangeReason', 'Unknown error')
                print(f"  Query failed: {reason}")
                
        except Exception as e:
            print(f"  Error: {e}")
    
    # Load into DuckDB
    if csv_files:
        print("\nLoading results into DuckDB...")
        sync_to_duckdb(csv_files)
        print(f"Data synced to {DUCKDB_PATH}")

if __name__ == '__main__':
    main()

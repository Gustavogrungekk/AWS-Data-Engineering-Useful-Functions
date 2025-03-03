#===================================================================================================================#
# Author: Gustavo Barreto
# Description: This Python library contains modules I've developed and use frequently. 
# Feel free to incorporate them into your own projects!
# Language: Python 3.9
# Date: 2024-06-19
# Version: 1.0
#===================================================================================================================#
# **Modules:**

# * **Data Engineering:**
#     1. sync_s3_bucket: This function syncs files from an S3 bucket to a local directory.
#     2. cdp_to_s3: This function transfers data from a CDP (Cloudera Data Platform) Hive table to S3 and optionally registers it as a table in the AWS Glue Data Catalog.
#     3. s3_contains: This function verifies if an S3 path exists.
#     4. try_date: This function tries to convert a string to a date.
#     5. clear_string: This function removes special characters from a string.
#     6. estimate_size: This function estimates the size of a DataFrame in a human-readable format using SI units.
#     7. success: This function uploads a _SUCCESS file to an S3 path.
#     8. check_success: This function checks if the _SUCCESS.txt file exists in the specified S3 path.
#     9. create_update_table: This function will mimic the behavior of aws glue crawler replication to update a table in aws glue catalog or create it if it doesn't exist.
#     10. get_last_modified_date: This function will check for the lattest modfied date in the s3 bucket for objects
#     11. get_table: This function will retrieve a table from the AWS Glue Data Catalog.
#     12. get_business_days: This function returns a list of business days between start_date and end_date for a given country.
#     13. pack_libs: This function packs python libraries and store them in S3.
#     14. aws_sso_login: This function automates the AWS SSO login process.
#     15. list_s3_size: This function lists the size of files in an S3 bucket.
#     16. save_athena_results: This function saves the results of an Athena query to an S3 path.
#     17. log: This function logs a message using the provided Glue context.
#     18. copy_redshift: This function copies data from S3 to Redshift.
#     19. get_partition: This function fetches the last partition of a table.
#     20. job_report: This function generates a report for the specified AWS Glue jobs, compiling details such as run date, start and end times, job status, and more.
#     21. restore_deleted_objects_S3: This function restores all deleted objects in an S3 bucket with versioning enabled.
#     22. get_calatog_latest_partition: This function returns the latest partition of a table in the AWS Glue Data Catalog.
#     23. athena_query_audit_report: This function generates an audit report for an Athena query.
#     24. get_s3_objects_details: This function returns details of objects in an S3 bucket.
#     25. monitor_state_machines: This function monitors the state machine executions and returns the latest execution details.
#     26. list_and_upload_files: This function lists files in a local directory path based on extension or name and upload them to S3.
#     27. process_local_files: This function processes local files stored in S3 and registers them in the Glue Data Catalog.

# * **Auxiliary Functions:**
#     - 1. convert_bytes: This function convert bytes into human readable format
#     - 2. get_bucket: This function get bucket name and prefix from s3 path given a full s3 uri path
#     - 3. run_athena_query: This function run an athena query and wait for it to finish
#     - 4. clear_s3_bucket: This function will clear an S3 bucket
#     - 5. s3_updown: This function downloads or uploads files from/to S3
#     - 6. list_glue_jobs: This function lists all Glue jobs
#     - 7. estimate_glue_job_cost: This function estimates the cost of a Glue job

# Dependency Libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from io import BytesIO
import re
import unicodedata
import holidays
import math
import csv
from datetime import datetime, timedelta, date, timezone
from time import sleep
from dateutil.relativedelta import relativedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import boto3
import os
import psycopg2
import repartipy
import pytz
import pyarrow.parquet as pq
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException

# ===================================================# Auxiliary Functions #===================================================#
# Aux 1. Convert bytes into human readable format
def convert_bytes(byte: int):
    '''
    Description: Convert bytes into human readable format
    Args:
        bytez: number of bytes
    How to use:
        convert_bytes(bytez=398345)
    '''
    if byte == 0:
        return '0B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(byte, 1024)))
    p = math.pow(1024, i)
    s = round(byte / p, 2)
    return '%s %s' % (s, size_name[i])

# Aux 2. Get bucket name and prefix from s3 path
def get_bucket(s3_uri):
    '''
    Description: Get bucket name and prefix from s3 path
    '''
    if not s3_uri.startswith('s3://'):
        raise ValueError(f'S3 path is either invalid or wrong s3 path: {s3_uri}')
    bucket = s3_uri[5:].split('/')[0]
    prefix = s3_uri.split(bucket)[1].lstrip('/')

    return bucket, prefix

# Aux 3. Run an athena query and wait for it to finish, return the query execution id
def run_athena_query(query: str, workgroup: str = 'primary'):
    '''
    Description: Run an athena query and wait for it to finish
    return the query execution id 
    '''

    athena = boto3.client('athena', region_name='sa-east-1')
    response = athena.start_query_execution(QueryString=query, WorkGroup=workgroup)
    while True:
        query_status = athena.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        state = query_status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        sleep(5)

    if state != 'SUCCEEDED':
        raise Exception(f'Query failed: {response}')
    return response['QueryExecutionId']

# Aux 4. Clear S3 bucket
def clear_s3_bucket(bucket_name: str):
    '''
    Description:
    This function will clear the specified S3 bucket
    
    Args:
        bucket_name: name of the bucket to be cleared

    Example:
        clear_s3_bucket('s3://bucket_name/dumps/athena-query-results/')
    '''
    bucket, prefix = get_bucket(bucket_name)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    # Delete all objects under the specified prefix
    objects_to_delete = bucket.objects.filter(Prefix=prefix)
    for obj in objects_to_delete:
        obj.delete()
    print(f'Successfully cleared S3 bucket: {bucket_name}')

# Aux 5. Download or upload files from/to S3
def s3_updown(s3_uri:str, local_path:str, method:str = 'download'):
    '''
    Description: Download or upload files from/to S3. 
    Must specify 'download' or 'upload'

    Args:
        s3_uri: S3 path
        local_path: Local path
        method: 'download' or 'upload'

    How to use:
        s3_updown(s3_uri, local_path, 'download')
    '''
    bucket, prefix = get_bucket(s3_uri)
    if method == 'download':
        s3 = boto3.client('s3')
        s3.download_file(bucket, prefix, local_path)
    elif method == 'upload':
        s3 = boto3.client('s3')
        s3.upload_file(local_path, bucket, prefix)
    else:
        raise ValueError(f'Invalid method: {method}')
    
# Aux 6. List all Glue jobs
def list_jobs(suffix:str, region_name:str='sa-east-1') -> list:
    '''
    Description: List all Glue jobs in a region if suffix is '*'
    returns a list of at least 1000 job names
    Parameters:
        suffix: str - Job name suffix to filter
    Returns:
        list - List of job names
    '''
    try:
        glue = boto3.client('glue', region_name=region_name)
        jobs = glue.list_jobs(MaxResults=1000).get('JobNames')
        if suffix.strip().lower() == '*':
            return jobs
        else:
            return list(set(filter(lambda job: suffix.strip().lower() in job.lower(), jobs)))
    except Exception as e:
        print(f'Error listing jobs: {str(e)}')

# Aux 7. Estimate the cost of a Glue job
def estimate_job_cost(job_name: str, RunId: str, region_name: str) -> float:
    '''
    Description: Estimates the cost of a Glue job running on AWS Glue. If no worker type is specified, PythonShell is assumed.
    Parameters:
        job_name: The name of the Glue job
        RunId: The ID of the Glue job run
        region_name: The AWS region where the Glue job is running
    Returns:
        The estimated cost of the Glue job
    '''
    costs = {
        'G.1X': 0.44,
        'G.2X': 0.88,
        'G.4X': 1.76,
        'G.8X': 3.52,
        'G.16X': 7.04,
        'G.32X': 14.08,
        'Standard': 0.44,
        'PythonShell': 0.44
    }
    glue = boto3.client('glue', region_name=region_name)
    try:
        response = glue.get_job_run(JobName=job_name, RunId=RunId)
        job_run = response['JobRun']

        duration_hours = job_run.get('ExecutionTime', 0) / 3600
        dpu = job_run.get('MaxCapacity', 0)
        try:
            worker_type = job_run['WorkerType']
        except:
            worker_type = 'Standard'

        cost = costs[worker_type] * dpu * duration_hours
        return cost
    except Exception as e:
        print(f"Error: {e}")
        return 0.0

    
# ===================================================# Awesome Modules #===================================================#

# 1. sync_s3_bucket
def sync_s3_bucket(S3_uri: str, Output_location: str):
    
    """
    Sync files from an S3 bucket to a local directory.

    Args:
        S3_uri (str): S3 URI to sync from (e.g., 's3://my-bucket/my-prefix/').
        Output_location (str): Local directory to sync to.

    Returns:
        str: Success message indicating all files have been downloaded.

    Raises:
        ValueError: If the S3 URI is invalid.
        Exception: If listing objects from the S3 bucket fails.
    
    Example:
        result = sync_s3_bucket('s3://my-bucket/my-prefix/', '/local/output/path')
        print(result)
        'All files downloaded successfully to /local/output/path.'
    """
    
    if not S3_uri.startswith('s3://'):
        raise ValueError(f'S3 path is either invalid or wrong s3 path: {S3_uri}')
    
    bucket = S3_uri[5:].split('/')[0]
    prefix = S3_uri.split(bucket)[1].lstrip('/')
    
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                
                local_filename = os.path.join(Output_location, key[len(prefix):])
                local_dir = os.path.dirname(local_filename)
                
                if not os.path.exists(local_dir):
                    try:
                        os.makedirs(local_dir)
                    except OSError as e:
                        print(f'Failed to create directory {local_dir}, error: {e}')
                        continue
                
                # Download the object
                try:
                    if not key.endswith('/'):  # Ignore directories
                        s3.download_file(Bucket=bucket, Key=key, Filename=local_filename)
                        print(f'Object: {key[len(prefix):]} downloaded successfully to {local_filename}.')
                except Exception as e:
                    print(f'Failed to download {key[len(prefix):]}, error: {e}')
                    continue
        else:
            raise Exception(f'Failed to list objects, empty directory or invalid prefix: {S3_uri}.')
    
    return f'All files downloaded successfully to {Output_location}.'

# 2. cdp_to_s3
def cdp_to_s3(username: str,
              passkey: str,
              jdbc_url: str,
              query: str,
              s3_output: str,
              writemode: str,
              catalog_table: bool = False,
              schema_name: str = None,
              table_name: str = None,
              partitionby: str = None,
              optional_athena_path: str = None,
              spark=None):
    """
    Transfer data from a CDP (Cloudera Data Platform) Hive table to S3 and optionally
    register it as a table in the AWS Glue Data Catalog.

    Args:
        username (str): Username for JDBC authentication.
        passkey (str): Password or key for JDBC authentication.
        jdbc_url (str): JDBC connection URL to the Hive server.
        query (str): SQL query to retrieve data from the Hive table.
        s3_output (str): S3 path where the data will be written.
        writemode (str): Write mode for the data transfer and table creation ('append', 'overwrite', etc.).
        catalog_table (bool, optional): If True, registers the data as a table in the AWS Glue Data Catalog. Default is False.
        schema_name (str, optional): Name of the schema in the Glue catalog where the table will be registered. Required if catalog_table is True.
        table_name (str, optional): Name of the table to be registered in the Glue catalog. Required if catalog_table is True.
        partitionby (str, optional): Column name(s) to partition the data by before writing to S3 and registering as a table.
        optional_athena_path (str, optional): Custom path to use for the Athena table.
        spark (SparkSession, optional): Existing SparkSession. If None, a new SparkSession will be created.

    Returns:
        str: Success message if data transfer and optional table registration are successful, or error message if failed.

    Raises:
        ValueError: If catalog_table is True but schema_name or table_name is not provided.

    Example:
        result = cdp_to_s3(username="your_username",
                           passkey="your_password",
                           jdbc_url="jdbc:hive2://your_hive_server:10000/default",
                           query="SELECT * FROM your_hive_table",
                           s3_output="s3://your-bucket/output-path/",
                           writemode="overwrite",
                           catalog_table=True,
                           schema_name="your_schema",
                           table_name="your_table",
                           partitionby="date_column")
        print(result)
    'Data transferred successfully to s3://your-bucket/output-path/'
    """
    if spark is None:
        spark = SparkSession.builder \
            .appName("cdp_to_s3") \
            .enableHiveSupport() \
            .getOrCreate()
        created_spark = True
    else:
        created_spark = False

    try:
        # Read data from Hive using JDBC and the specified query
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", username) \
            .option("password", passkey) \
            .option("driver", 'org.apache.hive.jdbc.HiveDriver') \
            .load()

        # Write DataFrame to S3
        if partitionby:
            df.write.mode(writemode).partitionBy(partitionby).parquet(s3_output)
        else:
            df.write.mode(writemode).parquet(s3_output)

        if catalog_table:
            # Prepare options for saving as a table
            save_options = {
                "path": optional_athena_path if optional_athena_path else s3_output,
                "format": "parquet",
                "mode": writemode,
                "compression": "snappy"
            }

            # Add partitionBy option if specified
            if partitionby:
                save_options["partitionBy"] = partitionby

            # Save DataFrame as table in Glue catalog
            if schema_name and table_name:
                df.write.mode(writemode).options(**save_options).saveAsTable(f"{schema_name}.{table_name}")
            else:
                raise ValueError("Both schema_name and table_name must be provided for catalog_table option")
        
        return f'Data transferred successfully to {s3_output}'

    except Exception as e:
        return f'Failed to transfer data: {str(e)}'

    finally:
        if created_spark:
            spark.stop()

    
# 3. s3_contains
def s3_contains(s3_path: str) -> bool:
    '''
    Description: Verifies if an S3 path exists.

    Args:
        s3_path (str): The S3 path to check.

    Returns:
        bool: True if the S3 path exists, False otherwise.

    Raises:
        ValueError: If the S3 path is invalid.
    '''

    bucket, prefix = get_bucket(s3_path)
    s3 = boto3.client('s3')

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            return True
        else:
            return False
    except Exception as e:
        print(f'Failed to list objects: {str(e)}')
        return False
    
# 4. try_date
def try_date(date_str: str, format = '%Y-%m-%d'):
    """
    Converts various date string formats to 'YYYY-MM-DD' format in Python datetime.date object.

    Args:
        date_str (str): Date string in various formats.

    Returns:
        datetime.date: Python datetime.date object.
    """

    # List of possible date formats to try parsing
    date_formats = [
        '%Y%m%d',                   # YYYYMMDD
        '%Y-%m-%d %H:%M:%S.%f %Z',  # YYYY-MM-DD HH:MM:SS.microseconds timezone
        '%Y-%m-%d',                 # YYYY-MM-DD
        '%-m/%-d/%Y',               # M/D/YYYY (allowing single digit month and day)
        '%d %B, %Y',                # D Month, YYYY (e.g., 1 January, 2024)
        '%Y-%m-%d %H:%M:%S',        # YYYY-MM-DD HH:MM:SS
        '%Y-%m-%d %H:%M:%S.%f',     # YYYY-MM-DD HH:MM:SS.microseconds
        '%Y-%m-%d %H:%M:%S.%f%z',   # YYYY-MM-DD HH:MM:SS.microseconds+HHMM
        '%Y-%m-%d %H:%M:%SZ',       # YYYY-MM-DD HH:MM:SSZ (UTC)
        '%Y-%d-%m',                 # YYYY-DD-MM
        '%d-%m-%Y',                 # DD-MM-YYYY
        '%s',                       # POSIX timestamp (seconds since 1970-01-01 00:00:00 UTC)
        '%Q',                       # POSIX timestamp with milliseconds (milliseconds since 1970-01-01 00:00:00 UTC)
        '%Y/%m/%d',                 # YYYY/MM/DD
        '%d-%b-%Y',                 # DD-Mon-YYYY (e.g., 01-Jan-2024)
        '%d-%B-%Y',                 # DD-Month-YYYY (e.g., 01-January-2024)
        '%b %d, %Y',                # Mon DD, YYYY (e.g., Jan 01, 2024)
        '%B %d, %Y',                # Month DD, YYYY (e.g., January 01, 2024)
        '%d/%m/%Y',                 # DD/MM/YYYY
        '%d-%m-%y',                 # DD-MM-YY (two-digit year)
        '%m/%d/%Y',                 # MM/DD/YYYY
        '%m-%d-%y',                 # MM-DD-YY (two-digit year)
        '%d/%m/%y',                 # DD/MM/YY (two-digit year)
        '%m-%d-%Y',                 # MM-DD-YYYY
        '%y-%m-%d',                 # YY-MM-DD (two-digit year)
        '%y/%m/%d',                 # YY/MM/DD (two-digit year)
        ]
    
    # Attempting to parse the date using each format
    for fmt in date_formats:
        try:
            # Strips any leading or trailing whitespace characters
            cleaned_date_str = date_str.strip()
            
            # Handles Unix timestamps as strings (case of '%s' and '%Q')
            if fmt in ['%s', '%Q']:
                match = re.match(r'^\d{10,13}$', cleaned_date_str)
                if not match:
                    continue
            
            return datetime.strptime(cleaned_date_str, fmt).date().strftime(format=format)
        
        except ValueError:
            continue
    
    # If none of the formats match, return None or raise an Exception as needed
    raise ValueError(f"Unable to parse date string: {date_str}")

# 5. clear_string
def clear_string(text: str, remove: set = None) -> str:
    """
    Clears accentuations, special characters, emojis, etc. from the input text and removes specified characters.

    Args:
        text (str): Text that will be cleaned.
        remove (set, optional): A set containing the elements to be found and removed from the text. Defaults to an empty set.

    Returns:
        str: The cleaned text.

    Usage Example:
        remove_set = {'#^$%@#^|][]'}
        clear_string('AAç#^$úcar Ca%@#^|][]nção', remove_set)
        # Output: 'Acucar Cancao'
    """
    if text:
        # Normalize the string to separate base characters from their combining marks
        normalized_text = unicodedata.normalize('NFD', text)
        # Removing Combining Marks:
        ascii_text = ''.join([c for c in normalized_text if unicodedata.category(c) != 'Mn'])
        # Removing non-printable characters
        clean_text = ''.join([c for c in ascii_text if unicodedata.category(c)[0] != 'C'])
        
        # Set default for remove parameter if not provided
        if remove is None:
            remove = set()
        
        # Removing specified characters
        clean_text = ''.join([c for c in clean_text if c not in remove])
        
        return clean_text

    return text

# 6. estimate_size
def estimate_size(spark, df)-> str: 
    """
    Estimates the size of a DataFrame in a human-readable format using SI units.

    Parameters:
        spark (SparkSession): The Spark session.
        df (DataFrame): The DataFrame to estimate the size of.

    Returns:
        str: The estimated size of the DataFrame in a human-readable format.
    """
    with repartipy.SizeEstimator(spark=spark, df=df) as se:
        df_size_in_bytes = se.estimate()

    # Ensure the size is an integer or float
    if not isinstance(df_size_in_bytes, (int, float)):
        raise TypeError("df_size_in_bytes must be an integer or float.")

    return convert_bytes(byte=df_size_in_bytes)

# 7. success
def success(s3_path:str=None):
    """
    Send a _SUCCESS.txt file to the specified S3 path.

    Parameters:
        s3_path (string) - The S3 Path where the _SUCCESS File should be sent.
    
    Returns:
        str: Success message if the file is uploaded, or an error message if it fails.
    """
    # Extracting bucket name and prefix from the provided s3 path:
    bucket, prefix = get_bucket(s3_path)
    try:
        s3 = boto3.client('s3')
        s3.put_object(Body=b'', Bucket=bucket, Key=f'{prefix}_SUCCESS')
        print(f'Successfully sent _SUCCESS to {s3_path}')
        return
    except Exception as e:
        return f'Failed to send _SUCCESS.txt to {s3_path}: {e}'
    
# 8. check_success
def check_success(s3_path:str=None):
    """
    Check if the _SUCCESS.txt file exists in the specified S3 path.

    Parameters:
        s3_path (string) - The S3 Path where the _SUCCESS File should be sent.

    Returns:
        bool: True if the _SUCCESS.txt file exists, False if it doesn't.
    """
    # Extracting bucket name and prefix from the provided s3 path:
    bucket, prefix = get_bucket(s3_path)
    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket=bucket, Key=f'{prefix}_SUCCESS')
        return True
    except Exception as e:
        return False
    
# 9. create_update_table
def create_update_table(database_name:str,
                          table_name:str,
                          s3_path:str,
                          file_format:str,
                          region:str = 'us-east-1',
                          spark=None):
    '''
    Description:
    This function will mimic the behavior of aws glue crawler replication to update a table in aws glue catalog or create it if it doesn't exist
    If the table already exists, it will update it else it will create it in the aws glue catalog

    Args:
        spark: spark session
        database_name: name of the database where the table will be created
        table_name: name of the table to be created
        s3_path: s3 location where the table will be created
        file_format: parquet or csv
        region: region where the table will be created by default is us-east-1

    returns:
        A string with the success message
        How to use:
        spark = spark_session()
        create_update_table(spark, database_name, table_name, s3_path, file_format)
    '''

    glue = boto3.client('glue', region_name=region)

    if spark is None:
        spark = SparkSession.builder.appName('create_update_table').getOrCreate()

    if file_format == 'parquet':    
        df = spark.read.parquet(s3_path)
    elif file_format == 'csv':
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
    else:
        raise ValueError("Unsupported file format")

    # Infer schema
    schema = df.schema

    # Try to create or update the Glue table
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        table_exists = True
    except glue.exceptions.EntityNotFoundException:
        table_exists = False

    # Convert schema to Glue-compatible format
    glue_columns = []
    for field in schema.fields:
        column_type = field.dataType.simpleString()
        if column_type == 'string':
            column_type = 'STRING'
        elif column_type == 'integer':
            column_type = 'INT'
        elif column_type == 'double':
            column_type = 'DOUBLE'
        elif column_type == 'float':
            column_type = 'FLOAT'
        elif column_type == 'timestamp':
            column_type = 'TIMESTAMP'
        elif column_type == 'date':
            column_type = 'DATE'
        elif column_type == 'boolean':
            column_type = 'BOOLEAN'
        elif column_type == 'binary':
            column_type = 'BINARY'
        elif column_type == 'array':
            column_type = 'ARRAY'
        elif column_type == 'map':
            column_type = 'MAP'
        elif column_type == 'struct':
            column_type = 'STRUCT'
        else:
            raise ValueError(f"Unsupported column type: {column_type}")

        glue_columns.append(glue.Column(name=field.name, type=column_type))

    # Create or update the Glue table
    if table_exists:
        glue.update_table(DatabaseName=database_name, TableInput=glue.TableInput(Name=table_name, Columns=glue_columns))
    else:
        glue.create_table(DatabaseName=database_name, TableInput=glue.TableInput(Name=table_name, Columns=glue_columns))

    return f'Table {table_name} saved in {s3_path} successfully'

# 10. get_last_modified_date
def get_last_modified_date(s3_path, time_interval:int=None, region = 'us-east-1'):
    '''
    Description:
    This function will check for the lattest modfied date in the s3 bucket for objects

    Args:
        s3_path: s3 location where the table will be created
        time_interval: time interval in hours optional
        region: region where the table will be created by default is us-east-1

    returns:
        A string with the last modified date
        How to use:
        get_last_modified_date(s3_path)
    '''

    bucket, key = get_bucket(s3_path)
    s3 = boto3.client('s3', region_name=region)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    if 'Contents' in response:
        last_modified = max([obj['LastModified'] for obj in response['Contents']])
        if time_interval:
            last_modified = last_modified - timedelta(hours=time_interval)
    else:
        last_modified = None
    return last_modified.strftime('%Y-%m-%d %H:%M:%S') 
    
# 11. get_table
def get_table(database_name: str, table_name: str, view: str = None, dataframe: str = None, options: dict = None, region: str = 'us-east-1', spark=None, glueContext=None):
    '''
    Description:
    This function will create a view or a spark dataframe based on AWS Glue create_dynamic_frame.from_catalog function

    Args:
        spark (SparkSession): Spark session
        database_name (str): Name of the database where the table will be created
        table_name (str): Name of the table to be created
        view (str): Name of the view to create
        dataframe (str): Name of the dataframe to create
        options (dict): Options for creating the dynamic frame
        region (str): AWS region

    Returns:
        A view or a spark dataframe

    How to use:
        get_table(database_name='my_database', table_name='my_table', view='my_view')
    '''

    if spark is None:
        spark = SparkSession.builder.appName('get_table').getOrCreate()

    if glueContext is None:
        glueContext = GlueContext(spark.sparkContext)

    if options is None:
        options = {}

    if view:
        df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name, transformation_ctx=view, **options).toDF()
        df.createOrReplaceTempView(view)
        print(f'View {view} created successfully')
        return df
    elif dataframe:
        df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name, transformation_ctx=dataframe, **options).toDF()
        print(f'Dataframe {dataframe} created successfully')
        return df
    else:
        raise ValueError("Must specify either view or dataframe")

# 12. get_business_days
def get_business_days(country: str, start_date: str, end_date: str) -> list:
    """
    Returns a list of business days between start_date and end_date for a given country.

    Parameters:
        country (str): The country code (e.g., 'US' for the United States, 'UK' for the United Kingdom, 'BR' for Brazil).
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        list: A list of business days (in 'YYYY-MM-DD' format) between the given dates for the specified country.

    Example:
        get_business_days('BR', '2024-01-01', '2024-07-31')
    """
    
    # Convert start and end dates to datetime
    start = pd.to_datetime(start_date) 
    end = pd.to_datetime(end_date) 
    all_days = pd.date_range(start=start, end=end, freq='D') # Generate all days between start and end

    # Get the country's holidays
    country_holidays = holidays.CountryHoliday(country)
    business_days = [day for day in all_days if day.weekday() < 5 and day not in country_holidays]
    business_days_str = [day.strftime('%Y-%m-%d') for day in business_days]

    return business_days_str

# 13. pack_libs
def pack_libs(libname: str, format: str, s3_path: str, requirements: str = 'requirements.txt'):
    '''
    Description: I've developed this function to automate the packing of python libraries and store them in S3.
    So you can use it whenever you want.

    Args:
        libname: name of the library
        format: format of the library ex: zip
        s3_path: path of the library in S3 ex: s3://bucket/prefix
        requirements: name of the requirements file ex: requirements.txt make sure to store it in the same folder as the library containing the libraries you want to pack.

    How to use:
        pack_libs(libname='libs', format='zip', s3_path='s3://gustavogrungekk/libs/')
    '''
    import boto3
    import subprocess
    import shutil
    import os
    import sys
    try:
        # Initialize S3 client
        s3 = boto3.client('s3')
        bucket = s3_path[5:].split('/')[0]
        prefix = s3_path.split(bucket)[1].lstrip('/')
        print(f"Downloading {requirements} from {s3_path}...")
        s3.download_file(bucket, f'{prefix}{requirements}', requirements)
        print(f"Installing libraries to {libname}...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', requirements, '--target', libname])
        print(f"Packing libraries into {libname}.{format}...")
        shutil.make_archive(libname, format, root_dir=libname)
        print(f"Uploading {libname}.{format} to {s3_path}...")
        s3.upload_file(f'{libname}.{format}', bucket, f'{prefix}{libname}.{format}')
        print("Cleaning up local files...")
        shutil.rmtree(libname)
        os.remove(f'{libname}.{format}')
        os.remove(requirements)
        print(f"{libname}.{format} uploaded to {s3_path}")
        return f'Successfully packed {libname}.{format} and uploaded to {s3_path}'
    except Exception as e:
        print(f"An error occurred: {e}")
        return str(e)

# 14. aws_sso_login
def aws_sso_login(profile_name:str,
                sso_start_url:str, 
                sso_region:str, 
                account_id:str, 
                role_name:str):
    import subprocess

    '''
    Descriptino: Automate the AWS SSO login process.

    Args:
        profile_name: Name of the AWS CLI profile.
        sso_start_url: AWS SSO start URL.
        sso_region: AWS SSO region.
        account_id: AWS account ID.
        role_name: AWS IAM role name.

    How to use:
        aws_sso_login(profile_name='default', sso_start_url='https://my-sso-portal.awsapps.com/start', sso_region='us-west-2', account_id='123456789012', role_name='MyRole')
    '''
    configure_sso_command = [
        'aws', 'configure', 'set', f'profile.{profile_name}.sso_start_url', sso_start_url,
        'aws', 'configure', 'set', f'profile.{profile_name}.sso_region', sso_region,
        'aws', 'configure', 'set', f'profile.{profile_name}.sso_account_id', account_id,
        'aws', 'configure', 'set', f'profile.{profile_name}.sso_role_name', role_name
    ]
    
    for i in range(0, len(configure_sso_command), 3):
        subprocess.run(configure_sso_command[i:i+3], check=True)
    login_command = ['aws', 'sso', 'login', '--profile', profile_name]
    subprocess.run(login_command, check=True)
    print(f"Logged in to AWS account {account_id} with profile {profile_name}.")

# 15. list_s3_size
def list_s3_size(s3_path: str, showdir: bool = False):
    '''
    Description: List the size of files in an S3 bucket.
    Args:
        s3_path: Path of the S3 bucket ex: s3://bucket/prefix.
        showdir: If True, shows the size of each individual file.
    How to use:
        list_size(s3_path='s3://bucket/prefix')
    '''
    bucket, prefix = get_bucket(s3_path)
    
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    total_size = 0
    total_objects = 0

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
                total_objects += 1
                if showdir:
                    size_str = convert_bytes(obj['Size'])
                    print(f"{obj['Key']}: {size_str}")

    size_converted = convert_bytes(total_size)
    print(f'Total size: {size_converted}\nTotal Objects: {total_objects}')

# 16. save_athena_results
def save_athena_results(query,
                        s3_path: str,
                        s3_athena_output: str,
                        writemode: str = 'append',
                        workgroup: str = 'primary',
                        workspace: str = None,
                        table_name: str = None,
                        partition: list = None,
                        spark=None):
    if spark is None:
        spark = SparkSession.builder.appName('save_athena_results').getOrCreate()

    # Execute the query on Athena
    query_id = run_athena_query(query, workgroup)

    try:
        # Collect the output from S3
        output_path = f'{s3_athena_output}/{query_id}'
        table = spark.read.format('csv').option('header', 'true').load(output_path)

        if partition and not workspace and not table_name:
            table.write.mode(writemode).partitionBy(partition).parquet(s3_path)
        elif not partition and not workspace and not table_name:
            table.write.mode(writemode).parquet(s3_path)
        elif workspace and table_name:
            table.write.mode(writemode).partitionBy(partition if partition else []).format('parquet').option('path', s3_path).saveAsTable(f'{workspace}.{table_name}')

            # Running MSCK REPAIR TABLE command
            run_athena_query(f'MSCK REPAIR TABLE {workspace}.{table_name}', workgroup)
        return f'Data saved successfully to {s3_path}'

    except Exception as e:
        return f'Failed to save results: {str(e)}'
    
# 17. log
def log(text: str, glueContext:GlueContext=None):
    """
    Description: Log a message using the provided Glue context.
    If no Glue context is provided, create one using the default Spark context.

    Args:
        text (str): The message to log.
        glueContext (GlueContext, optional): The Glue context to use for logging and monitoring.
    """
    if glueContext is None:
        glueContext = GlueContext(SparkContext.getOrCreate())
    
    logger = glueContext.get_logger()
    logger.info(f"{'*' * 35} {text} {'*' * 35}")

# 18. copy_redshift a very old function of mine back when I was just an assistant an old but reliable function.
def copy_redshift(s3_path: str, schema:str, redshift_table:str, writemode:str='append', spark=None):
    """
    Description:
    Copies data from S3 to Redshift. This function reads the data from S3, prepares the metadata, 
    and loads the data into a specified Redshift table. It handles creating the table if it doesn't exist.

    Args:
    s3_path (str): Full path to your data (example: 's3://gold-datalake/data/*/*')
    schema (str): The schema where you want to copy the data to
    redshift_table (str): The name of the table to create/copy data to
    writemode (str): Writing mode - 'overwrite' or 'append'

    Usage Example:
    copy_redshift(s3_path='s3://gold-datalake/data/*/*',
                  schema='public',
                  redshift_table='copied_table_test',
                  writemode='append')
    """
    
    redshift_table = f'{schema}.{redshift_table}'.lower()

    # Getting the credentials 
    if spark is None:
        spark = SparkSession.builder.appName('copy_redshift').getOrCreate()

    red_path = 's3://your-bucket/credentials/redshift_credentials_example.csv'
    cred = spark.read.options(header=True, delimiter=';').csv(red_path)

    credencial = {}
    for field in cred.select('field').collect():
        credencial[f'{field[0]}'] = cred.filter(col('field') == f'{field[0]}').select('key').collect()[0][0]

    redshift_host = credencial['host']
    redshift_db = 'gold-datalake'
    redshift_user = credencial['user']
    redshift_password = credencial['password']
    redshift_iam_role = credencial['iam']
    out_query = credencial['query_output']
    redshift_port = 5439

    url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}?user={redshift_user}&password={redshift_password}"
    
    # Uploading the metadata to Redshift.
    LOCATION = s3_path
    df = spark.read.format('parquet').load(LOCATION)
    
    # Creating a new column to upload the metadata/schema.
    df = df.filter('0=1')
    
    # Establishing a connection with the DB.
    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    cursor = conn.cursor()
    
    # Trying to truncate the existing table    
    try:
        query_truncate_table = f"TRUNCATE {redshift_table}"
        cursor.execute(query_truncate_table)
        conn.commit()
    except Exception as e:
        conn.rollback() # In case the table does not exist and cannot be truncated, we will create it.
        df.write\
           .format("jdbc")\
           .option("url", url)\
           .option("dbtable", redshift_table)\
           .option("tempdir", out_query)\
           .option("aws_iam_role", redshift_iam_role)\
           .mode(writemode)\
           .save()

    # Adapting the string for copy command.
    copy_format = r'/?(?:year=\d{4}/month=\d{2}|\d{4}/\d{2}|\*/\*|\*)/?'
    s3_path = re.sub(copy_format, '/', s3_path)

    query_copy_parquet = f"""
        COPY {redshift_table}
        FROM '{s3_path}'
        IAM_ROLE '{redshift_iam_role}'
        ACCEPTINVCHARS
        PARQUET;
    """
    # Copy data from Parquet files in S3 to Redshift Commit and close the connection.
    cursor.execute(query_copy_parquet)
    conn.commit()
    conn.close()

# 19. get_partition
def get_partition(table: str, delimiter: str = '/', spark=None):
    '''
    Description: This function fetches the last partition of a table.
    Args:
        table: Table name
        delimiter: Delimiter used in partition keys
        spark: Existing Spark session (optional)

    Example: 
        get_partition(table='my_table')
    '''
    
    # Create a SparkSession if none is provided
    if not spark:
        spark = SparkSession.builder.appName("get_partitions").getOrCreate()

    # Fetch partitions
    partitions = spark.sql(f"SHOW PARTITIONS {table}").rdd.flatMap(lambda x: x).collect()

    if not partitions:
        return None

    # Get the last partition
    last_partition = sorted(partitions)[-1]

    # Replace delimiter with " and " in the partition string
    last_p = last_partition.replace(delimiter, " and ")

    return last_p

# 20. job_report
def get_glue_job_audit_report(jobnames, region_name='us-east-1', inactive_days=31, hours_interval=12):
    """
    Generates an audit report for AWS Glue jobs by retrieving job details, execution statistics, and associated costs.

    Args:
        jobnames (list): A list of AWS Glue job names to retrieve audit information for.
        region_name (str, optional): The AWS region to connect to. Defaults to 'us-east-1'.
        inactive_days (int, optional): The number of days to consider a job as 'inactive'. Defaults to 31.
        hours_interval (int, optional): The interval in hours for filtering job runs. Defaults to 12.

    Returns:
        pd.DataFrame: A DataFrame containing the job audit report with details such as job status, execution time, 
                      cost, data processed, records processed, and trigger information.
    """

    glue = boto3.client('glue', region_name=region_name)
    current_time = datetime.now(timezone.utc)
    costs = {
        'G.1X': 0.44,
        'G.2X': 0.88,
        'G.4X': 1.76,
        'G.8X': 3.52,
        'G.16X': 7.04,
        'G.32X': 14.08,
        'Standard': 0.44,
        'PythonShell': 0.44,
    }
    job_details = []

    for jobname in jobnames:
        try:
            job_response = glue.get_job(JobName=jobname)
            job_definition = job_response['Job']
            created_at = job_definition['CreatedOn']
            timeout = job_definition.get('Timeout', None)
            glue_version = job_definition.get('GlueVersion', None)
            execution_mode = job_definition.get('ExecutionClass', 'Standard')

            # Fetch all triggers and filter by jobname
            triggers = glue.get_triggers()
            trigger_name = None
            for trigger in triggers['Triggers']:
                if jobname in [action['JobName'] for action in trigger.get('Actions', [])]:
                    trigger_name = trigger['Name']
                    break
            
            job_runs = glue.get_job_runs(JobName=jobname)['JobRuns']
            for run in job_runs:
                start_time = run['StartedOn']
                end_time = run.get('CompletedOn', None)
                job_state = run['JobRunState']
                is_active = (current_time - start_time) <= timedelta(days=inactive_days)
                runtime_seconds = run.get('ExecutionTime', 0)
                runtime_hours = runtime_seconds / 3600
                error_message = run.get('ErrorMessage', None)

                worker_type = run.get('WorkerType', 'Standard')
                if worker_type == 'Standard':
                    worker_type = 'G.1X'

                dpu = run.get('MaxCapacity', 0)
                cost = costs.get(worker_type, 0) * dpu * runtime_hours

                job_details.append({
                    'job_name': jobname,
                    'job_type': 'Glue Job',
                    'job_date_creation': created_at.strftime('%Y-%m-%d'),
                    'run_date': start_time.strftime('%Y-%m-%d'),
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else None,
                    'glue_job_status': job_state,
                    'active': is_active,
                    'worker_type': worker_type,
                    'dpu': dpu,
                    'runtime': f"{runtime_hours:.2f} hours",
                    'execution_mode': execution_mode,
                    'glue_version': glue_version,
                    'timeout': timeout,
                    'cost': round(cost, 2),
                    'error_message': error_message,
                    'trigger_name': trigger_name,
                    'data_processed': run.get('Metrics', {}).get('DataProcessed', {}).get('BYTES', 0),
                    'records_processed': run.get('Metrics', {}).get('RecordsProcessed', {}).get('COUNT', 0),
                    'anomesdia': start_time.strftime('%Y%m%d')
                })

        except Exception as e:
            print(f"Error processing job {jobname}: {e}")

    return pd.DataFrame(job_details)

# 21. restore_deleted_objects_S3
def restore_deleted_objects_S3(bucket_name, prefix='', time=None):
    """
    Restore all deleted objects in an S3 bucket with versioning enabled.
    If bucket versioning is not enabled... damn dude.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str, optional): The prefix of the objects to restore. Defaults to ''.
        time (int, optional): The amount of hours to search for deleted objects. Finds the objects deleted within the last `time` hours. Defaults to None.

    How to use:
        restore_deleted_objects_S3(bucket_name, prefix='', time=None)    

    Tips:
        - time is in hours. If you want to find the objects deleted within the last 24 hours, set time = 24.
        - prefix is the prefix of the objects to restore. If you want to restore all objects in the bucket, set prefix = ''.
    """
    s3 = boto3.client('s3')
    current_time = datetime.now(timezone.utc)

    try:
        paginator = s3.get_paginator('list_object_versions')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in page_iterator:
            deleted_objects = page.get('DeleteMarkers', [])
            versions = page.get('Versions', [])

            for deleted_object in deleted_objects:
                key = deleted_object['Key']
                delete_marker_time = deleted_object['LastModified']
                
                if time is not None:
                    time_delta = timedelta(hours=time)
                    if current_time - delete_marker_time > time_delta:
                        continue

                # Find the latest version that is not a delete marker
                latest_version = None
                for version in versions:
                    if version['Key'] == key and not version.get('IsDeleteMarker', False):
                        latest_version = version
                        break

                if latest_version:
                    version_id = latest_version['VersionId']
                    
                    # Copy the latest version back to the original key
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': key, 'VersionId': version_id},
                        Key=key)
                    
                    print(f"Restored {key} to its latest non-deleted version (Version ID: {version_id}).")
                else:
                    print(f"No non-deleted version found for {key}")

        print("All deleted objects have been processed.")
    
    except NoCredentialsError:
        print("Error: No AWS credentials found. Please configure your AWS credentials.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials found. Please check your AWS credentials.")
    except Exception as e:
        print(f"An error occurred: {e}")

# 22. get_calatog_latest_partition
def get_calatog_latest_partition(database_name, table_name, athena_workgroup, region_name='sa-east-1'):
    """
    This function will return the latest partition of a table in AWS Glue Catalog
    """
    
    OutputLocation = f"s3://athena-query-results-{boto3.Session().region_name}/{database_name}/".lower()
    glue_client = boto3.client('glue', region_name)
    athena_client = boto3.client('athena', region_name)

    # Get partition keys and their types from Glue catalog
    try:
        glue_table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        partition_keys = [
            {"name": key['Name'], "type": key['Type']} 
            for key in glue_table['Table'].get('PartitionKeys', [])
        ]
    except Exception as e:
        raise Exception(f"Failed to fetch table metadata from Glue: {str(e)}")

    if not partition_keys:
        raise Exception(f"Table '{table_name}' in database '{database_name}' has no partitions.")

    # Construct the ORDER BY clause dynamically
    order_by_clause = ", ".join([f"{key['name']} DESC" for key in partition_keys])

    query = f"""
        SELECT *
        FROM "{database_name}"."{table_name}$partitions"
        ORDER BY {order_by_clause}
        LIMIT 1;
    """

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            WorkGroup=athena_workgroup,
            ResultConfiguration={'OutputLocation': OutputLocation}
        )
        query_execution_id = response['QueryExecutionId']
    except Exception as e:
        raise Exception(f"Failed to execute Athena query: {str(e)}")
    
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        sleep(3)

    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {state}")
    try:
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']
    except Exception as e:
        raise Exception(f"Failed to fetch query results: {str(e)}")

    if len(rows) <= 1:  # Header row only
        raise Exception(f"No partitions found for table '{table_name}'.")

    # Extract partition keys and values
    partition_values = {key['VarCharValue']: value['VarCharValue'] for key, value in zip(rows[0]['Data'], rows[1]['Data'])}
    
    # Build partition clause dynamically based on types
    partition_conditions = []
    for partition_key in partition_keys:
        key_name = partition_key["name"]
        key_type = partition_key["type"].lower()
        key_value = partition_values[key_name]
        
        if "int" in key_type:  # Integer types (int, bigint, smallint, etc.)
            partition_conditions.append(f"{key_name}={key_value}")
        elif "float" in key_type or "double" in key_type:  # Floating-point types
            partition_conditions.append(f"{key_name}={key_value}")
        elif "boolean" in key_type:  # Boolean type
            partition_conditions.append(f"{key_name}={key_value.lower()}")
        elif "timestamp" in key_type:  # Timestamp type
            partition_conditions.append(f"{key_name}='{key_value}'")
        elif "date" in key_type:  # Date type
            partition_conditions.append(f"{key_name}='{key_value}'")
        elif "char" in key_type or "string" in key_type:  # Character or string types
            partition_conditions.append(f"{key_name}='{key_value}'")
        elif "decimal" in key_type:  # Decimal types
            partition_conditions.append(f"{key_name}={key_value}")
        else:  # Default case: handle as a string
            partition_conditions.append(f"{key_name}='{key_value}'")

    partition_clause = " AND ".join(partition_conditions)
    return partition_clause

# 23. athena_query_audit_report
def athena_query_audit_report(region:str, hours_interval:int, workgroups:list):
    '''
    Description:
        This function will return a report of all queries executed in a given region and workgroups.

    Args:
        region (str): AWS region name.
        hours_interval (int): Number of hours to look back.
        workgroups (list): List of workgroup names.

    Returns:
        report_data (list): List of dictionaries containing query details.
    '''

    athena_client = boto3.client('athena', region_name=region)

    # Calculate the time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_interval)

    report_data = []

    for workgroup in workgroups:
        # Fetch query execution IDs for the current workgroup
        executions = athena_client.list_query_executions(WorkGroup=workgroup)
        query_ids = executions.get('QueryExecutionIds', [])

        for query_id in query_ids:
            query_execution = athena_client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

            # Extract required fields
            query_start_time = query_execution['Status']['SubmissionDateTime']
            if not (start_time <= query_start_time <= end_time):
                continue

            query_end_time = query_execution['Status'].get('CompletionDateTime')
            run_date = query_start_time.strftime('%Y-%m-%d') if query_start_time else None
            query = query_execution['Query'].replace('\n', ' ')
            status = query_execution['Status']['State']
            scanned_bytes = query_execution['Statistics'].get('DataScannedInBytes', 0)
            scanned_kb = round(scanned_bytes / 1024, 2)
            scanned_mb = round(scanned_kb / 1024, 2)
            scanned_gb = round(scanned_mb / 1024, 2)
            scanned_tb = round(scanned_gb / 1024, 2)
            database = query_execution['QueryExecutionContext'].get('Database')
            output_location = query_execution['ResultConfiguration'].get('OutputLocation')
            workgroup = query_execution['WorkGroup']
            estimated_cost = round(scanned_tb * 5, 2)  # Estimated $5 per TB scanned, rounded to 2 decimal places
            anomesdia = query_start_time.strftime('%Y%m%d') if query_start_time else None

            # Add record to report
            report_data.append({
                'query_id': query_id,
                'run_date': run_date,
                'start_time': query_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': query_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'query': query,
                'status': status,
                'scanned_bytes': scanned_bytes,
                'scanned_kb': scanned_kb,
                'scanned_mb': scanned_mb,
                'scanned_gb': scanned_gb,
                'database': database,
                'output_location': output_location,
                'workgroup': workgroup,
                'estimated_cost_usd': estimated_cost,
                'anomesdia': anomesdia
            })

    # Convert to DataFrame
    df = pd.DataFrame(report_data)

    return df

# 24. get_s3_objects_details
def get_s3_objects_details(bucket_names):
    """
    List all objects within given S3 buckets and return detailed information for analysis.

    Args:
        bucket_names (list): List of S3 bucket names.

    Returns:
        pd.DataFrame: DataFrame containing object details for the given buckets.
    """
    s3 = boto3.client('s3')
    current_time = datetime.now(pytz.UTC)  # Make current time aware (UTC)
    all_objects_details = []

    for bucket_name in bucket_names:  # Loop through each bucket name
        try:
            # Get the storage class of the bucket
            bucket_info = s3.head_bucket(Bucket=bucket_name)
            storage_class = bucket_info.get('StorageClass', 'STANDARD')  # Default to STANDARD if not found

            # List the objects in the bucket
            objects = s3.list_objects_v2(Bucket=bucket_name)
            if 'Contents' not in objects:
                print(f"No objects found in bucket: {bucket_name}")
                continue

            most_accessed_object = None
            most_accessed_object_count = 0  # To track the most accessed object
            for obj in objects['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified'].astimezone(pytz.UTC)  # Convert to UTC-aware datetime
                size_bytes = obj['Size']
                
                # Size formatting
                size_kb = size_bytes / 1024
                size_mb = size_kb / 1024
                size_gb = size_mb / 1024
                size_tb = size_gb / 1024
                size_pb = size_tb / 1024

                # Convert sizes to human-readable format
                size_kb = round(size_kb, 2)
                size_mb = round(size_mb, 2)
                size_gb = round(size_gb, 2)
                size_tb = round(size_tb, 2)
                size_pb = round(size_pb, 2)

                # Extract file extension (data type)
                file_extension = key.split('.')[-1] if '.' in key else 'unknown'

                # Check encryption status (server-side encryption)
                encryption_status = obj.get('ServerSideEncryption', 'None')

                # Calculate object age in days
                age = current_time - last_modified
                age_days = age.days

                # Retrieve data retention rule and expiration (if set)
                data_retention_rule = obj.get('Metadata', {}).get('dataRetentionRule', 'Not Set')
                object_expiration = obj.get('Expiration', 'Not Set')

                # Object tags (if any)
                try:
                    tags = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                    tags = tags.get('TagSet', [])
                except s3.exceptions.ClientError:
                    tags = []

                # Check for most accessed object
                if obj['Size'] > most_accessed_object_count:
                    most_accessed_object = key
                    most_accessed_object_count = obj['Size']

                # Append object details
                all_objects_details.append({
                    'bucket_name': bucket_name,
                    'storage_class': storage_class, 
                    'object_key': key,
                    'most_accessed_object': most_accessed_object,
                    'file_extension': file_extension,
                    'size_kb': f"{size_kb} KB",
                    'size_mb': f"{size_mb} MB",
                    'size_gb': f"{size_gb} GB",
                    'size_tb': f"{size_tb} TB",
                    'size_pb': f"{size_pb} PB",
                    'last_modified': last_modified.strftime('%Y-%m-%d %H:%M:%S'),
                    'object_age_days': age_days,
                    'encryption_status': encryption_status,
                    'tags': tags,
                    'data_retention_rule': data_retention_rule,
                    'object_expiration': object_expiration,
                    'anomesdia': datetime.now().strftime('%Y-%m-%d')
                })

        except Exception as e:
            print(f"Error processing bucket {bucket_name}: {e}")

    return pd.DataFrame(all_objects_details)

# 25. monitor_state_machines
def monitor_state_machines(state_machines, interval_hours=24, region_name='us-east-1'):
    """
    Monitor state machine executions and return a detailed report.

    :param state_machines: List of state machine ARNs or names.
    :param interval_hours: Time interval in hours to filter executions.
    :param region_name: AWS region where the state machine is located.
    :return: Pandas DataFrame with execution details.
    """
    client = boto3.client('stepfunctions', region_name=region_name)
    current_time = datetime.now(timezone.utc) 
    start_time = current_time - timedelta(hours=interval_hours)

    report_data = []

    for state_machine_arn in state_machines:
        try:
            # Get state machine details
            state_machine = client.describe_state_machine(stateMachineArn=state_machine_arn)
            state_machine_name = state_machine['name']
            definition = json.loads(state_machine['definition'])
            iam_role = state_machine['roleArn']

            # Detect related services using a generic regex
            related_services = set()
            arn_pattern = r"arn:aws:(\w+):[^:]+:.*" 
            for _, step in definition['States'].items():
                if step['Type'] == 'Task':  # Check if it's a task state
                    resource = step.get('Resource', '')
                    match = re.match(arn_pattern, resource)
                    if match:
                        related_services.add(match.group(1)) 

            # Fetch executions
            paginator = client.get_paginator('list_executions')
            page_iterator = paginator.paginate(
                stateMachineArn=state_machine_arn,
                maxResults=100
            )

            for page in page_iterator:
                for execution in page['executions']:
                    execution_arn = execution['executionArn']
                    status = execution['status']
                    start_date = execution['startDate']
                    stop_date = execution.get('stopDate', None)
                    anomesdia = start_date.strftime('%Y%m%d') 

                    # Initialize fields
                    step_statuses = {}
                    step_durations = {}
                    retried_steps = []
                    error_messages = []
                    error_codes = []

                    # Analyze history for detailed metrics
                    history = client.get_execution_history(
                        executionArn=execution_arn,
                        maxResults=1000,
                        reverseOrder=False
                    )['events']

                    for event in history:
                        if 'stateEnteredEventDetails' in event:
                            step_name = event['stateEnteredEventDetails']['name']
                            step_start = event['timestamp']
                            step_statuses[step_name] = 'STARTED'
                        if 'stateExitedEventDetails' in event:
                            step_name = event['stateExitedEventDetails']['name']
                            step_end = event['timestamp']
                            step_statuses[step_name] = 'SUCCEEDED'
                            step_durations[step_name] = (step_end - step_start).total_seconds()
                        if 'executionFailedEventDetails' in event:
                            error_message = event['executionFailedEventDetails'].get('cause', 'Unknown cause')
                            error_codes.append(event['executionFailedEventDetails'].get('error', 'Unknown error'))
                            error_messages.append(error_message)
                        if 'executionRetriedEventDetails' in event:
                            retried_steps.append(event['executionRetriedEventDetails'].get('name'))

                    # Calculate duration
                    if stop_date:
                        total_duration = (stop_date - start_date).total_seconds()
                        total_duration_minutes = total_duration / 60 
                        total_duration_hours = total_duration / 3600

                    report_row = {
                        "state_machine_arn": state_machine_arn,
                        "state_machine_name": state_machine_name,
                        "execution_arn": execution_arn,
                        "execution_name": execution['name'],
                        "start_date": start_date.strftime('%Y-%m-%d %H:%M:%S'),
                        "stop_date": stop_date.strftime('%Y-%m-%d %H:%M:%S') if stop_date else 'Still Running',
                        "status": status,
                        "step_statuses": step_statuses,
                        "step_durations": step_durations,
                        "retried_steps": retried_steps,
                        "error_messages": error_messages,
                        "error_codes": error_codes,
                        "related_services": list(related_services),
                        "iam_roles": iam_role,
                        "total_duration_seconds": total_duration, 
                        "total_duration_minutes": round(total_duration_minutes, 2), 
                        "total_duration_hours": round(int(total_duration_hours), 2),
                        "anomesdia": anomesdia, 
                    }

                    report_data.append(report_row)

        except Exception as e:
            print(f"Error processing state machine {state_machine_arn}: {e}")

    # Convert to DataFrame
    if report_data:
        df = pd.DataFrame(report_data)
    else:
        df = pd.DataFrame()

    return df

# 26. list_and_upload_files
def list_and_upload_files(config):
    """
    List files in a network/local directory path based on extension or name and upload them to S3.
    Returns a list of processed file names.

    :param config: Configuration dictionary containing necessary parameters.

    Example usage:
    # Example configuration
    config = {
        'network_path': r"", # Example: C:\Users\abc\data\files
        's3_bucket': "myawsbucket", # Bucket name
        's3_prefix': "Datasets/local_files/", # Example: Datasets/local_files/
        'file_extension': 'csv', # Change to 'xlsx' or 'parquet' if needed
        'file_name': ['iris'], # Use specific file name if required (list of names)
        'output_format': 'parquet', # This will ensure the output is always Parquet
        }

    uploaded_files, file_names = list_and_upload_files(config)
    print(f"Uploaded files: {uploaded_files}")
    print(f"Processed file names: {file_names}")

    """
    s3_client = boto3.client('s3')
    uploaded_files = []
    file_names = []  # This will store the names of files that are processed or uploaded

    # Get all the parameters from config dictionary
    network_path = config.get('network_path')
    s3_bucket = config.get('s3_bucket')
    s3_prefix = config.get('s3_prefix', '') # Optional, default to empty string if not provided
    file_extension = config.get('file_extension', None)
    file_name = config.get('file_name', None)
    output_format = config.get('output_format', 'parquet') # Default to 'parquet' if not provided

    # Collect files
    files = []
    for root, _, filenames in os.walk(network_path):
        for filename in filenames:
            if file_name and not any(fn in filename for fn in file_name):  # Handle file_name as a list
                continue
            if file_extension and not filename.endswith(f".{file_extension}"):
                continue
            files.append(os.path.join(root, filename))

    # Limit the number of files processed
    files = files[:config.get('max_files', 10)]  # Use config for max_files

    for file_path in files:
        try:
            file_name = os.path.basename(file_path)
            s3_key = os.path.join(s3_prefix, file_name)

            # Add file name to the list
            file_names.append(file_name)

            # Handle different file types and apply the output_format conversion early
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
                buffer = convert_to_output_format(df, output_format)
                s3_key = s3_key.replace('.csv', f'.{output_format}')
            
            elif file_path.endswith('.xlsx'):
                # Read all sheets into a dictionary of DataFrames
                excel_data = pd.read_excel(file_path, sheet_name=None)
                for sheet_name, df in excel_data.items():
                    sheet_s3_key = f"{s3_key}_{sheet_name}.{output_format}"
                    buffer = convert_to_output_format(df, output_format)
                    s3_client.put_object(Bucket=s3_bucket, Key=sheet_s3_key, Body=buffer)
                    uploaded_files.append(sheet_s3_key)  # Add sheet_s3_key to uploaded_files
                continue  # Skip further processing for .xlsx after sheets are uploaded

            elif file_path.endswith('.parquet'):
                table = pq.read_table(file_path)
                df = table.to_pandas()
                buffer = convert_to_output_format(df, output_format)
                s3_key = s3_key.replace('.parquet', f'.{output_format}')

            else:
                # For unsupported file types, upload as binary
                with open(file_path, 'rb') as file:
                    s3_client.upload_fileobj(file, s3_bucket, s3_key)
                uploaded_files.append(s3_key)
                continue

            # Upload the processed buffer to S3
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=buffer)
            uploaded_files.append(s3_key)
            print(f"Uploaded: {file_path} -> s3://{s3_bucket}/{s3_key}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    return uploaded_files, file_names

def convert_to_output_format(df, output_format):
    """
    Convert a DataFrame to the specified output format.
    :param df: The pandas DataFrame to be converted.
    :param output_format: The desired output format (parquet, csv, etc.)
    :return: The converted data in the desired format (as bytes).
    """
    if output_format == 'parquet':
        return df.to_parquet(engine='pyarrow', index=False)
    elif output_format == 'csv':
        return df.to_csv(index=False).encode('utf-8')
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

# 27. Process Local Files with Glue and stores in Glue Data Catalog
def process_local_files(config: dict):
    """
    Processes files stored in S3 and registers them in the Glue Data Catalog.

    Args:
        config (dict): A dictionary containing the configuration parameters:
            - s3_bucket (str): The S3 bucket where the files are stored.
            - s3_prefix (str): The S3 prefix for the files.
            - region (str): The AWS region where the S3 bucket and Glue service are located.
            - object_names (list): A list of object names (keys) within the S3 bucket and prefix.
            - table_suffix (str, optional): A suffix to be appended to the table name. If not specified,
              the table name will be based on the file name.
            - days_threshold (int): The number of days to check if the object is older. Objects older than
              this threshold will be skipped.

    Returns:
        None
    """

    # Get configuration parameters
    s3_bucket = config.get('s3_bucket')
    s3_prefix = config.get('s3_prefix')
    region = config.get('region')
    object_names = config.get('object_names')
    table_suffix = config.get('table_suffix', None)
    days_threshold = config.get('days_threshold', 1)  # Default to 1 day if not specified

    # Initialize boto3 S3 client
    s3_client = boto3.client('s3', region_name=region)

    # Initialize GlueContext and SparkContext
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # Loop through each file in the object_names list
    for object_name in object_names:
        try:
            # Construct the full S3 path
            full_s3_path = f"s3://{s3_bucket}/{s3_prefix}/{object_name}"

            # Get object metadata to check the LastModified timestamp
            response = s3_client.head_object(Bucket=s3_bucket, Key=f"{s3_prefix}/{object_name}")
            last_modified = response['LastModified']
            
            # Check if the file is older than the specified threshold
            if (datetime.now(last_modified.tzinfo) - last_modified).days > days_threshold:
                print(f"Skipping {object_name} as it is older than {days_threshold} days.")
                continue  # Skip processing this file

            # Determine file type based on extension
            file_type = object_name.split('.')[-1].lower()

            # Read the data from S3 based on the file type
            if file_type == 'csv':
                df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(full_s3_path)
            elif file_type == 'parquet':
                df = spark.read.parquet(full_s3_path)
            elif file_type == 'json':
                df = spark.read.json(full_s3_path)
            elif file_type in ['xls', 'xlsx']:
                df = spark.read.format('com.crealytics.spark.excel') \
                    .option('useHeader', 'true') \
                    .option('inferSchema', 'true') \
                    .load(full_s3_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            # Convert DataFrame to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

            # Create the table name, appending the suffix if provided
            if table_suffix:
                table_name = f"{table_suffix}{object_name.split('.')[0]}"
            else:
                table_name = f"{object_name.split('.')[0]}"  # Use file name without extension

            # Register the data in the Glue Data Catalog
            glueContext.write_dynamic_frame.from_catalog(
                frame=dynamic_frame,
                database="your_glue_database",  # Replace with your actual Glue database name
                table_name=table_name,
                transformation_ctx="DataCatalogOutput"
                )

            print(f"Processed and registered: {full_s3_path} as table '{table_name}'")

        except Exception as e:
            print(f"Error processing file: {full_s3_path}. Error: {e}")
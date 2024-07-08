#===================================================================================================================#
# Author: Gustavo Barreto
# Description: This Python library contains modules I've developed and use frequently. 
# Feel free to incorporate them into your own projects!
# Language: Python 3.9
# Date: 2024-06-19
# Version: 1.0
#===================================================================================================================#

def load_env(enable_spark: bool = False):
    '''
    Description:
    This function will load the dependencies for your project

    Args:
    enable_spark: boolean if you want to use pyspark

    How to use:
    load_dependencies(enable_spark=True|False)
    '''
    global pd, np, plt, sns, json, BytesIO, re, unicodedata, holidays, math, csv
    global datetime, timedelta, date, relativedelta, ClientError, boto3, os, sys, wr, psycopg2

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
    from datetime import datetime, timedelta, date
    from dateutil.relativedelta import relativedelta
    from botocore.exceptions import ClientError
    import boto3
    import os
    import sys
    import awswrangler as wr
    import repartipy
    import psycopg2

    if enable_spark:
        global spark, F, Window
        from pyspark.sql import SparkSession
        from pyspark.sql import types as T
        from pyspark.sql import functions as F
        from pyspark.sql import Window
        from pyspark.sql.utils import AnalysisException


        # Spark Session
        spark = SparkSession.builder.appName('SparkDevelopment') \
            .config('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'CORRECTED') \
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
            .config('spark.sql.catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
            .config('spark.delta.logStore.class', 'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore') \
            .config('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED') \
            .config('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'CORRECTED') \
            .config('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED') \
            .config('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
            .config('spark.driver.extraJavaOptions', '-XX:+UseG1GC') \
            .config('spark.dynamicAllocation.enabled', 'true') \
            .config('spark.dynamicAllocation.minExecutors', '1') \
            .config('spark.dynamicAllocation.maxExecutors', '10') \
            .config('spark.executor.memory', '4g') \
            .config('spark.driver.memory', '2g') \
            .config('spark.executor.cores', '2') \
            .config('spark.driver.cores', '1') \
            .config('spark.executor.instances', '4') \
            .config('spark.memory.fraction', '0.8') \
            .config('spark.memory.storageFraction', '0.2') \
            .config('spark.dynamicAllocation.initialExecutors', '2') \
            .config('spark.dynamicAllocation.executorIdleTimeout', '60s') \
            .config('maxPartitionBytes', '128MB') \
            .enableHiveSupport() \
            .getOrCreate()
        print("Spark session initialized with the specified configuration.")
        return spark

    print("Dependencies loaded.")


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
    >>> result = sync_s3_bucket('s3://my-bucket/my-prefix/', '/local/output/path')
    >>> print(result)
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
                
                # Construct the local filename
                local_filename = os.path.join(Output_location, key[len(prefix):])
                local_dir = os.path.dirname(local_filename)
                
                # Ensure local directory structure exists
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


def create_or_update_table(spark, glue_client, database_name, table_name, s3_path, file_format):
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
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table_exists = True
    except glue_client.exceptions.EntityNotFoundException:
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
        elif column_type == 'long':
            column_type = 'BIGINT'
        elif column_type == 'boolean':
            column_type = 'BOOLEAN'
        elif column_type == 'date':
            column_type = 'DATE'
        elif column_type == 'timestamp':
            column_type = 'TIMESTAMP'
        else:
            column_type = 'STRING'  # Default fallback
        glue_columns.append({'Name': field.name, 'Type': column_type})

    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': glue_columns,
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' if file_format == 'parquet' else 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' if file_format == 'parquet' else 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' if file_format == 'parquet' else 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'StoredAsSubDirectories': False
        },
        'TableType': 'EXTERNAL_TABLE'
    }

    if table_exists:
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
        print(f"Updated table {table_name} in database {database_name}")
    else:
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
        print(f"Created table {table_name} in database {database_name}")

def process_files(input_path, database_name):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Glue Crawler Replication") \
        .getOrCreate()

    # Initialize boto3 client
    glue_client = boto3.client('glue')

    # Iterate over files in the input directory
    for root, _, files in os.walk(input_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".parquet"):
                table_name = os.path.splitext(file)[0]
                create_or_update_table(spark, glue_client, database_name, table_name, file_path, 'parquet')
            elif file.endswith(".csv"):
                table_name = os.path.splitext(file)[0]
                create_or_update_table(spark, glue_client, database_name, table_name, file_path, 'csv')

    spark.stop()
from pyspark.sql import SparkSession

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
              spark: SparkSession = None):
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
    >>> result = cdp_to_s3(username="your_username",
                           passkey="your_password",
                           jdbc_url="jdbc:hive2://your_hive_server:10000/default",
                           query="SELECT * FROM your_hive_table",
                           s3_output="s3://your-bucket/output-path/",
                           writemode="overwrite",
                           catalog_table=True,
                           schema_name="your_schema",
                           table_name="your_table",
                           partitionby="date_column")
    >>> print(result)
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

def get_latest_partition(database_name, table_name, aws_region='us-west-2'):
    """
    Get the latest partition from an Athena table with multiple partition columns.

    Args:
    database_name (str): The name of the Athena database.
    table_name (str): The name of the Athena table.
    aws_region (str): AWS region where the Athena and Glue services are hosted. Default is 'us-west-2'.

    Returns:
    dict: The latest partition values as a dictionary.
    """

    # Create a Glue client
    glue_client = boto3.client('glue', region_name=aws_region)

    try:
        # Get table metadata
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        partitions = response['Table']['PartitionKeys']

        if not partitions:
            raise ValueError(f"No partition keys found for table '{table_name}' in database '{database_name}'.")

        # Extract partition column names
        partition_columns = [partition['Name'] for partition in partitions]

        # Connect to Athena
        conn = connect(region_name=aws_region, cursor_class=PandasCursor)

        # Build the query to get the latest partition
        select_columns = ', '.join(partition_columns)
        group_by_columns = ', '.join(partition_columns)
        max_column = f"MAX({partition_columns[-1]}) AS latest_partition"  # Assuming the last partition column is the timestamp or date column

        query = f"""
        SELECT {select_columns}, {max_column}
        FROM {database_name}.{table_name}
        GROUP BY {group_by_columns}
        ORDER BY latest_partition DESC
        LIMIT 1
        """

        # Execute the query
        df = pd.read_sql(query, conn)

        # Convert result to dictionary
        latest_partition = df.to_dict(orient='records')[0]

        return latest_partition

    except Exception as e:
        return f"Failed to get latest partition: {str(e)}"
    

def s3_contains(s3_path: str) -> bool:

    if not s3_path.startswith('s3://'):
        raise ValueError(f'S3 path is invalid: {s3_path}')

    # Extract bucket and prefix from the S3 path
    parts = s3_path[5:].split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    
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
    
def try_date(date_str: str, format = '%Y-%m-%d'):
    """
    Converts various date string formats to 'YYYY-MM-DD' format in Python datetime.date object.

    Args:
    - date_str (str): Date string in various formats.

    Returns:
    - datetime.date: Python datetime.date object.
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

def clear_string(text: str, remove: set = None) -> str:
    """
    Clears accentuations, special characters, emojis, etc. from the input text and removes specified characters.

    Args:
    - text (str): Text that will be cleaned.
    - remove (set, optional): A set containing the elements to be found and removed from the text. Defaults to an empty set.

    Returns:
    - str: The cleaned text.

    Usage Example:
    >>> remove_set = {'#^$%@#^|][]'}
    >>> clear_string('AAç#^$úcar Ca%@#^|][]nção', remove_set)
    'Acucar Cancao'
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

def upload_file_to_s3(file_location: str, bucket: str, object_path: str = None):
    '''
    Uploads a local file to S3

    Args:
    - file_location: Path to the local file
    - bucket: Name of the S3 bucket
    - object_path: Path and name of the file in S3 (optional)

    Returns:
    - str: Success or error message
    '''
    s3 = boto3.client('s3')
    
    if object_path is None:
        # If object_path is not specified, use the local file name
        object_path = file_location.split('/')[-1]
    
    try:
        s3.upload_file(file_location, bucket, object_path)
        return f'File {file_location} sent successfully to {bucket}/{object_path}'
    except Exception as e:
        return f'Error uploading file: {str(e)}'

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

    if df_size_in_bytes == 0:
        return "0B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(df_size_in_bytes, 1024)))
    p = math.pow(1024, i)
    s = np.round(df_size_in_bytes / p, 2)
    return f"{s} {size_name[i]}"
    

def success(s3_path:str=None):
    """
    Send a _SUCCESS.txt file to the specified S3 path.

    Parameters:
    s3_path (string) - The S3 Path where the _SUCCESS File should be sent.
    
    Returns:
    str: Success message if the file is uploaded, or an error message if it fails.
    """
    # Extracting bucket name and prefix from the provided s3 path:
    if not s3_path.startswith('s3://'):
        raise Exception("Invalid S3 Path!")

    bucket = s3_path[5:].split('/')[0]
    prefix = s3_path.split(bucket)[1].lstrip('/')
    try:
        s3 = boto3.client('s3')
        s3.put_object(Body=b'', Bucket=bucket, Key=f'{prefix}_SUCCESS')
        print(f'Successfully sent _SUCCESS to {s3_path}')
        return
    except Exception as e:
        return f'Failed to send _SUCCESS.txt to {s3_path}: {e}'
    

def check_success(s3_path:str=None):
    """
    Check if the _SUCCESS.txt file exists in the specified S3 path.

    Parameters:
    s3_path (string) - The S3 Path where the _SUCCESS File should be sent.

    Returns:
    bool: True if the _SUCCESS.txt file exists, False if it doesn't.
    """
    # Extracting bucket name and prefix from the provided s3 path:
    if not s3_path.startswith('s3://'):
        raise Exception("Invalid S3 Path!")

    bucket = s3_path[5:].split('/')[0]
    prefix = s3_path.split(bucket)[1].lstrip('/')
    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket=bucket, Key=f'{prefix}_SUCCESS')
        return True
    except Exception as e:
        return False
    
def create_or_update_table(spark, database_name:str, table_name:str, s3_path:str, file_format:str, region:str = 'us-east-1'):
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
    create_or_update_table(spark, database_name, table_name, s3_path, file_format)
    '''

    glue = boto3.client('glue', region_name=region)

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
    if not s3_path.startswith('s3://'):
        raise ValueError(f'S3 path must start with "s3://" got {s3_path}')
    
    bucket = s3_path[5:].split('/')[0]
    key = '/'.join(s3_path[5:].split('/')[1:])
    s3 = boto3.client('s3', region_name=region)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    if 'Contents' in response:
        last_modified = max([obj['LastModified'] for obj in response['Contents']])
        if time_interval:
            last_modified = last_modified - timedelta(hours=time_interval)
    else:
        last_modified = None
    return last_modified.strftime('%Y-%m-%d %H:%M:%S') 

def get_table(spark, database_name:str, table_name:str, view=None, dataframe=None, region:str = 'us-east-1'):
    '''
    Description:
    This function will create a view or a spark dataframe based on aws glue create_dynamic_frame.from_catalog function

    Args:
    spark: spark session
    database_name: name of the database where the table will be created
    table_name: name of the table to be created
    view: boolean if the table will be created as a view
    dataframe: boolean if the table will be created as a spark dataframe

    returns:
    A view or a spark dataframe
    How to use:
    get_dataframe(spark, database_name, table_name)
    '''
    glueContext = GlueContext(spark)
    if view:
        df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name, transformation_ctx=view).toDF().createOrReplaceTempView(table_name)
        print(f'View {table_name} created successfully')
        return df
    elif dataframe:
        df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name, transformation_ctx=dataframe).toDF()
        print(f'Dataframe {table_name} created successfully')
        return df
    raise ValueError("Must specify either view or dataframe")

def get_business_days(country: str, start_date: str, end_date: str) -> list:
    """
    Returns a list of business days between start_date and end_date for a given country.

    Parameters:
    - country (str): The country code (e.g., 'US' for the United States, 'UK' for the United Kingdom, 'BR' for Brazil).
    - start_date (str): The start date in 'YYYY-MM-DD' format.
    - end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
    - list: A list of business days (in 'YYYY-MM-DD' format) between the given dates for the specified country.

    Example:
    get_business_days('BR', '2024-01-01', '2024-07-31')
    """
    
    # Convert start and end dates to datetime
    start = pd.to_datetime(start_date) 
    end = pd.to_datetime(end_date) 
    all_days = pd.date_range(start=start, end=end, freq='D') # Generate all days between start and end

    # Get the country's holidays
    country_holidays = holidays.CountryHoliday(country)

    # Filter out weekends and holidays
    business_days = [day for day in all_days if day.weekday() < 5 and day not in country_holidays]

    # Convert business days to string format
    business_days_str = [day.strftime('%Y-%m-%d') for day in business_days]

    return business_days_str

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

def aws_sso_login(profile_name:str,
                sso_start_url:str, 
                sso_region:str, 
                account_id:str, 
                role_name:str):
    import subprocess

    '''
    Automate the AWS SSO login process.

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


def list_s3_size(s3_path: str, showdir: bool = False):
    '''
    Description: List the size of files in an S3 bucket.

    Args:
    s3_path: path of the S3 bucket ex: s3://bucket/prefix

    How to use:
    list_size(s3_path='s3://bucket/prefix')
    '''
    if not s3_path.startswith('s3://'):
        raise ValueError('S3 path is either invalid or wrong s3 path')
    
    bucket = s3_path[5:].split('/')[0]
    prefix = s3_path.split(bucket, 1)[1].lstrip('/')
    
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    total_size = 0
    total_objects = 0

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
                total_objects +=1
                if total_size > 0:
                    if showdir:
                        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
                        i = int(math.floor(math.log(total_size, 1024)))
                        p = math.pow(1024, i)
                        s = round(total_size / p, 2)
                        print(f"{obj['Key']}: {s}{size_name[i]}")
        
    if total_size > 0:
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(total_size, 1024)))
        p = math.pow(1024, i)
        s = round(total_size / p, 2)
        size_converted = f'{s}{size_name[i]}'
    else:
        size_converted = '0B'
    
    print(f'Total size: {size_converted}\nTotal Objects: {total_objects - 1}')


def run_athena_query(query: str, workgroup: str = 'primary'):
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

def save_athena_results(query,
                        s3_path: str,
                        s3_athena_output: str,
                        writemode: str = 'append',
                        workgroup: str = 'primary',
                        to_sas: bool = False,
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
        
        if to_sas and not workspace and not table_name and not partition:
            table.coalesce(1).write.mode(writemode).option('header', 'true').option('quote', '"').option('delimiter', '|').csv(s3_path)
        elif partition and not workspace and not table_name:
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
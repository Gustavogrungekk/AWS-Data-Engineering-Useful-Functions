#===================================================================================================================#
# Author: Gustavo Barreto
# Description: This Python library contains modules I've developed and use frequently. 
# Feel free to incorporate them into your own projects!
# Language: Python 3.9
# Date: 2024-06-19
# Version: 1.0
#===================================================================================================================#
# INDEX:
# 1. sync_s3_bucket: This function syncs files from an S3 bucket to a local directory.
# 2. cdp_to_s3: This function transfers data from a CDP (Cloudera Data Platform) Hive table to S3 and optionally registers it as a table in the AWS Glue Data Catalog.
# 3. s3_contains: This function verifies if an S3 path exists.
# 4. try_date: This function tries to convert a string to a date.
# 5. clear_string: This function removes special characters from a string.
# 6. estimate_size: This function estimates the size of a DataFrame in a human-readable format using SI units.
# 7. success: This functino will upload a _SUCCESS file to an S3 path.
# 8. check_success: This function checks if the _SUCCESS.txt file exists in the specified S3 path.
# 9. create_update_table: This function will mimic the behavior of aws glue crawler replication to update a table in aws glue catalog or create it if it doesn't exist.
# 10. get_last_modified_date: This function will check for the lattest modfied date in the s3 bucket for objects
# 11. get_table: This function will retrieve a table from the AWS Glue Data Catalog.
# 12. get_business_days: This function returns a list of business days between start_date and end_date for a given country.
# 13. pack_libs: This function packs python libraries and store them in S3.
# 14. aws_sso_login: This function automates the AWS SSO login process.
# 15. list_s3_size: This function lists the size of files in an S3 bucket.
# 16. save_athena_results: This function saves the results of an Athena query to an S3 path.
# 17. log: This function logs a message using the provided Glue context.
# 18. copy_redshift: This function copies data from S3 to Redshift.
# 19. get_partition: This function fetches the last partition of a table.
# ===================================================================================================================#
# Auxiliary Functions
# 1. convert_bytes: This function convert bytes into human readable format
# 2. get_bucket: This function get bucket name and prefix from s3 path given a full s3 uri path
# 3. run_athena_query: This function run an athena query and wait for it to finish
# 4. clear_s3_bucket: This function will clear an S3 bucket
# 5. s3_updown: This function downloads or uploads files from/to S3
# ===================================================================================================================#

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
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import boto3
import os
import psycopg2
import repartipy
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

    
# 3. s3_contains
def s3_contains(s3_path: str) -> bool:
    '''
    Description: Verifies if an S3 path exists.

    Args:
    - s3_path (str): The S3 path to check.

    Returns:
    - bool: True if the S3 path exists, False otherwise.

    Raises:
    - ValueError: If the S3 path is invalid.
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

# 5. clear_string
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
    Log a message using the provided Glue context.
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



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def get_partition(table: str, delimiter: str = '/', spark=None):
    '''
    Description: This function fetches the last partition of a table.
    Args:
        table: Table name
        delimiter: Delimiter used in partition keys
        spark: Existing Spark session (optional)

    Returns:
        - str: Last partition string

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

    # Define a function to parse and normalize partition keys
    def parse_partition(partition):
        # Split partition string by delimiter
        parts = partition.split(delimiter)
        # Normalize each key-value pair
        parts_normalized = []
        for part in parts:
            key, value = part.split('=')
            parts_normalized.append(f"{key}={int(value)}" if value.isdigit() else f"{key}={value}")
        # Join normalized parts back into a partition string
        return delimiter.join(parts_normalized)

    # Normalize partition values and get the latest partition
    latest_partition = (spark.read.option("delimiter", delimiter)
                        .csv(spark.sparkContext.parallelize(partitions), schema="partition string")
                        .withColumn("partition_normalized", expr("substring(`partition string`, 13)"))
                        .orderBy(expr("partition_normalized"))
                        .first()["partition string"])

    return latest_partition

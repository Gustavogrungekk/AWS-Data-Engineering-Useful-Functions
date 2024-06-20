import boto3
import os
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pandas as pd

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
              optional_athena_path: str = None):
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
    spark = SparkSession.builder \
        .appName("cdp_to_s3") \
        .enableHiveSupport() \
        .getOrCreate()

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
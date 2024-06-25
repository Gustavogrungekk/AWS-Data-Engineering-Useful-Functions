import boto3
import os

def sync_s3_bucket(S3_uri: str, Output_location: str):
    
    '''
    S3_uri: S3 URI to sync from (e.g., 's3://my-bucket/my-prefix/')
    Output_location: Local directory to sync to
    '''
    
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



# Define schema
schema = StructType([
    StructField("error", StructType([
        StructField("error_details", StructType([
            StructField("string", StringType(), True),
            StructField("struct", StructType([
                StructField("field1", StringType(), True),
                StructField("field2", StringType(), True)
            ]), True)
        ]), True)
    ]), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the original DataFrame
df.show(truncate=False)
df.printSchema()

# Function to recursively extract all field names from a nested struct
def get_nested_fields(schema, prefix=""):
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields.extend(get_nested_fields(field.dataType, field_name))
        else:
            fields.append(field_name)
    return fields

# Get all nested fields from the 'error' column
nested_fields = get_nested_fields(schema["error"].dataType, prefix="error")

# Select all nested fields dynamically
selected_columns = [col(field).alias(field.replace(".", "_")) for field in nested_fields]
df_extracted = df.select(*selected_columns)

# Show the extracted DataFrame
df_extracted.show(truncate=False)
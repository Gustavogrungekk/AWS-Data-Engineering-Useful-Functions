from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import psycopg2

def copy_parquet_to_redshift(s3_bucket, s3_key,
                              redshift_table,
                              aws_access_key_id,
                              aws_secret_access_key,
                              redshift_host, 
                              redshift_port,
                              redshift_iam_role, 
                              redshift_db, 
                              redshift_user, 
                              redshift_password):
    
    # Writing the medata to Redshift
    LOCATION = s3_bucket + s3_key
    df = spark.read.format('parquet').load(LOCATION)

    # Creating a new column to upload the metada
    df = df.withColumn('Number_1', lit(None))

    # Filtering for a non existent number in the new column so we can have an empty dataframe/metadata only
    df = df.filter(col('Number_1') == 1).drop('Number_1')

    # Uploading the metadata to Redshift
    df.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}") \
    .option("dbtable", f"{redshift_table}") \
    .option("tempdir", f"s3://{s3_bucket}/temp") \
    .option("aws_iam_role", f"{redshift_iam_role}") \
    .option("aws_access_key_id", aws_access_key_id) \
    .option("aws_secret_access_key", aws_secret_access_key) \
    .mode("overwrite") \
    .save()  

    
    # Create a Redshift connection
    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port)

    cursor = conn.cursor()

    # Truncate the Redshift table
    query_truncate_table = f"TRUNCATE {redshift_table}"
    cursor.execute(query_truncate_table)

    # Copy data from Parquet files in S3 to Redshift
    query_copy_parquet = f"""
        COPY {redshift_table}
        FROM 's3://{s3_bucket}/{s3_key}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        IAM_ROLE '{redshift_iam_role}'
        PARQUET;
    """
    cursor.execute(query_copy_parquet)

    # Commit and close the connection
    conn.commit()
    conn.close()

# Example usage:
s3_bucket = 'your-s3-bucket'
s3_key = 'your-s3-key.parquet'
redshift_table = 'your_redshift_table'
aws_access_key_id = 'your-aws-access-key-id'
aws_secret_access_key = 'your-aws-secret-access-key'
redshift_host = 'your-redshift-host'
redshift_port = 'your-redshift-port'
redshift_db = 'your-redshift-database'
redshift_user = 'your-redshift-user'
redshift_password = 'your-redshift-password'
redshift_iam_role = 'your-redshift-iam-role'

copy_parquet_to_redshift(s3_bucket, s3_key,
                        redshift_table,
                        aws_access_key_id, 
                        aws_secret_access_key,
                        redshift_host, 
                        redshift_port, 
                        redshift_db, 
                        redshift_user, 
                        redshift_password)

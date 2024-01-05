#===============================================================================================================================================#
# Description: Load and transform your PySpark dataframe stored in the efficient .parquet format, into AWS Redshift.
# Author: Gustavo Barreto
# Date: 10/08/2023
# Languages: Python3, boto3
#===============================================================================================================================================#
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import psycopg2
import re

spark = SparkSession.builder \
    .config('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'CORRECTED') \
    .config('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED') \
    .appName('Redshift-copy-function') \
    .getOrCreate()

def copy_redshift(s3_path=str,
                            schema=str,
                            redshift_table=str,
                            writemode=str):
    '''
    s3_path = Full path to your data example: s3://gold-datalake/data/*/*'
    schema = The schema where you want to copy the data to
    redshift_table = The name of the table we will be creating/copying
    writemode = Writting mode 'overwrite' or 'append'
    '''
    
    """
    Usage Example:
    copy_redshift(s3_path=s3://gold-datalake/data/*/*',
                        schema='public',
                        redshift_table='copied_table_test',
                        writemode='append')
    """
    
    redshift_table = f'{schema}.{redshift_table}'.lower()

    # Getting the credentials 
    red_path = 's3://your-bucket/credentials/redshift_credentials_example.csv'
    cred = spark.read.options(header = 'True', delimiter = ';').csv(red_path)

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

    url = f"jdbc:redshift://{redshift_host}:5439/gold-datalake?user={redshift_user}&password={redshift_password}"
    
    # Uploading the metadata to Redshift
    LOCATION = s3_path
    df = spark.read.format('parquet').load(LOCATION)
    
    # Creating a new column to upload the metada
    df = df.withColumn('Fictional_Number_hash64_1x', lit(None))

    # Filtering for a non existent number in the new column so we can have an empty dataframe/metadata only
    df = df.filter(col('Fictional_Number_hash64_1x') == 1).drop('Fictional_Number_hash64_1x')   
    
    # Estabilshing a connection with the DB
    conn = psycopg2.connect(
    dbname=redshift_db,
    user=redshift_user,
    password=redshift_password,
    host=redshift_host,
    port=redshift_port)
    cursor = conn.cursor()
    
    # Trying to truncate the existing table    
    try:
        query_truncate_table = f"TRUNCATE {redshift_table}"
        cursor.execute(query_truncate_table)
        conn.commit() 
    except:
        conn.rollback() # In case the table does not exists and cannot be truncated, we will create it
        df.write\
           .format("jdbc")\
           .option("url", f"{url}")\
           .option("dbtable", f"{redshift_table}")\
           .option("tempdir", f"{out_query}")\
           .option("aws_iam_role", f"{redshift_iam_role}")\
           .mode(f"{writemode}")\
           .save()\

    # Adapting the string for copy command
    copy_format = r'(?:\d{4}/\d{2}/)|(\*/\*)$'
    s3_path = re.sub(copy_format, '', s3_path)

    query_copy_parquet = f"""
        COPY {redshift_table}
        FROM '{s3_path}'
        IAM_ROLE '{redshift_iam_role}'
        ACCEPTINVCHARS
        PARQUET;
    """
    # Copy data from Parquet files in S3 to Redshift Commit and close the connection
    cursor.execute(query_copy_parquet)
    conn.commit()
    conn.close()
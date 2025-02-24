import boto3
import time
import pandas as pd
from datetime import datetime

# AWS Clients
def get_aws_client(service, region='sa-east-1'):
    return boto3.client(service, region_name=region)

# Get table metadata from AWS Glue
def get_glue_table_info(database, table_name, region='sa-east-1'):
    glue = get_aws_client('glue', region)
    try:
        response = glue.get_table(DatabaseName=database, Name=table_name)
        return response['Table']
    except Exception as e:
        print(f"Error fetching metadata for {database}.{table_name}: {str(e)}")
        return None

# Get partition keys dynamically
def get_partition_keys(table_info):
    return [p['Name'] for p in table_info.get('PartitionKeys', [])]

# Get latest partition dynamically
def get_latest_partition(database, table_name, partition_keys, region='sa-east-1'):
    glue = get_aws_client('glue', region)
    try:
        if not partition_keys:
            return None  # Table is not partitioned

        response = glue.get_partitions(DatabaseName=database, TableName=table_name)
        partitions = response.get('Partitions', [])

        if not partitions:
            return None

        # Extract partition values dynamically
        partition_list = []
        for partition in partitions:
            values = partition['Values']
            partition_list.append(tuple(values))  # Keep as strings

        # Sort partitions by tuple order (latest first)
        partition_list.sort(reverse=True)

        # Get latest partition values
        latest_partition_values = partition_list[0]
        partition_filters = " AND ".join(f"{k} = '{v}'" for k, v in zip(partition_keys, latest_partition_values))

        return partition_filters

    except Exception as e:
        print(f"Error fetching latest partition for {database}.{table_name}: {str(e)}")
        return None

# Build the quality check query for the latest partition
def build_quality_query(table_info, latest_partition_filter):
    database = table_info['DatabaseName']
    table_name = table_info['Name']

    where_clause = f"WHERE {latest_partition_filter}" if latest_partition_filter else ""

    return f"""
    SELECT
        '{datetime.now().strftime('%Y%m%d')}' AS anomesdia,
        '{table_name}' AS table_name,
        '{database}' AS table_schema,
        COUNT(*) AS records
    FROM {database}.{table_name}
    {where_clause}
    """

# Execute Athena query and return execution ID
def start_athena_query(query, database, region='sa-east-1', output_location='s3://athena-query-results/'):
    athena = get_aws_client('athena', region)
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        return response['QueryExecutionId']
    except Exception as e:
        print(f"Error starting Athena query for {database}: {str(e)}")
        return None

# Wait for Athena query execution
def wait_for_query(execution_id, region='sa-east-1', max_attempts=30, sleep_seconds=5):
    athena = get_aws_client('athena', region)
    for _ in range(max_attempts):
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status == 'SUCCEEDED'
        time.sleep(sleep_seconds)
    return False

# Retrieve Athena query results as a DataFrame
def get_query_results(execution_id, region='sa-east-1'):
    athena = get_aws_client('athena', region)
    results = []
    next_token = None
    
    while True:
        kwargs = {'QueryExecutionId': execution_id}
        if next_token:
            kwargs['NextToken'] = next_token
            
        response = athena.get_query_results(**kwargs)
        columns = [col['Name'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        
        for row in response['ResultSet']['Rows'][1:]:
            values = [field.get('VarCharValue', '') for field in row['Data']]
            results.append(dict(zip(columns, values)))
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return pd.DataFrame(results)

# Main function to execute quality checks for the latest partition
def run_quality_check(tables: list, region='sa-east-1', output_location='s3://athena-query-results/'):
    all_results = []
    
    for table in tables:
        try:
            database, table_name = table.split('.')
            
            # Get metadata
            table_info = get_glue_table_info(database, table_name, region)
            if not table_info:
                continue

            # Get partition keys dynamically
            partition_keys = get_partition_keys(table_info)
            
            # Get latest partition dynamically
            latest_partition_filter = get_latest_partition(database, table_name, partition_keys, region)

            if latest_partition_filter:
                print(f"Latest partition for {table}: {latest_partition_filter}")
            else:
                print(f"No partitions found for {table}, using full table scan!")

            # Build query
            query = build_quality_query(table_info, latest_partition_filter)
            
            # Execute query
            execution_id = start_athena_query(query, database, region, output_location)
            if not execution_id:
                continue
            
            print(f"Query started for {table} (ID: {execution_id})")
            
            # Wait for execution
            if wait_for_query(execution_id, region):
                # Fetch results
                df = get_query_results(execution_id, region)
                all_results.append(df)
            else:
                print(f"Query failed for {table}")
                
        except Exception as e:
            print(f"Error processing table {table}: {str(e)}")
    
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

# Example usage
if __name__ == "__main__":
    tables_list = [
        "my_database.table1",
        "my_database.table2",
        "another_db.table3"
    ]
    df_results = run_quality_check(tables_list)
    print(df_results)
